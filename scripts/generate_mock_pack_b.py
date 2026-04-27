#!/usr/bin/env python
"""Generate internally-consistent mock data for Pack B (6 assets, 10 ready_now demos).

Pack B unlocks 10/13 ready_now initiatives by mocking these 6 source tables
with shared business keys and domain-plausible measure relationships:

  ll_quote_policy_detail               — pricing decomposition (Liberty share, USD)
  ll_quote_coverage_detail             — coverage / exposure / limit / deductible
  ll_quote_setup                       — quote header dimensions (broker, UW, dates)
  rate_monitoring                      — risk-adjusted rate change at LAYER grain (renewal-only)
  rate_monitoring_total_our_share_usd  — quote-grain rate-change roll-up (renewal-only)
  quote_policy_detail                  — full-share twin of ll_quote_policy_detail

Generation strategy:
  1. Build a quote_dim of 250 quotes with stable (quote_id, pas_id, broker, UW,
     section, currency, dates).
  2. For each quote, generate 1-3 layers; for each layer, 1-2 coverages.
     This produces ~500 layer rows and ~750 coverage rows, with tuples that
     join cleanly across the 5 layer/coverage-grain assets.
  3. Generate measures with deliberate insurance-domain relationships:
       tech_gnwp >= modtech_gnwp >= sold_gnwp  (typical softening market)
       commission ~ 10-25% of sold_gnwp
       gross_rarc skewed slightly negative (-0.15 to +0.15)
       claims_inflation 2-8%, deductible/excess/limit hierarchy enforced
  4. The full-share twin (quote_policy_detail) carries the same key tuples and
     scales measures up by 1/our_share_pct (so ll_* sums to a fraction of the
     full-share row's measures, as Liberty's signed-line economics imply).
  5. Rate monitoring is generated layer-grain first (one row per renewing
     layer, with independent rate-change components per layer). The
     quote-grain `rate_monitoring_total_our_share_usd` seed is then derived
     by **premium-weighted aggregation** of the layer-grain rows, weighted by
     each layer's `tech_gnwp_full`. This makes the two seeds reconcile by
     construction: a consumer summing weighted layer rates back up to quote
     grain will exactly match the quote-grain seed.

Output: output/mock_data/pack_b/<asset_name>.csv

Run with:
  .venv/Scripts/python.exe scripts/generate_mock_pack_b.py
"""

import csv
import math
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 42
random.seed(SEED)

OUT_DIR = Path(__file__).parent.parent / "dbt_demo" / "seeds"
OUT_DIR.mkdir(parents=True, exist_ok=True)

N_QUOTES = 250
PDM_TS = "2026-04-25 22:00:00.000"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _lognormal(median: float, sigma: float) -> float:
    mu = math.log(median)
    return math.exp(random.gauss(mu, sigma))

def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))

def _round(value: float, ndp: int = 2) -> float:
    return round(value, ndp)

def _date_iso(d: date) -> str:
    return d.isoformat()

def _bool_str(b: bool) -> str:
    return "true" if b else "false"

def _write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

# ---------------------------------------------------------------------------
# Reference dimensions
# ---------------------------------------------------------------------------

SECTIONS = [
    "European Professional Indemnity",
    "Directors and Officers",
    "General Aviation",
    "Cash in Transit and Specie",
    "Contingency",
]

COVERAGES_BY_SECTION = {
    "European Professional Indemnity": ["Professional Indemnity", "Cyber"],
    "Directors and Officers": ["Side A", "Side B/C", "Entity Securities"],
    "General Aviation": ["Hull", "Liability", "War"],
    "Cash in Transit and Specie": ["Cash in Transit", "Vault"],
    "Contingency": ["Event Cancellation", "Non-Appearance"],
}

CURRENCIES = ["USD", "USD", "USD", "GBP", "EUR"]  # weighted to USD
BROKERS = [
    "Marsh", "Aon", "WTW", "Lockton", "Howden",
    "Gallagher", "BMS", "McGill", "Tysers",
]
UNDERWRITERS = [
    "James Whitfield", "Sarah Patel", "Michael Chen", "Emma Robinson",
    "David Okonkwo", "Priya Iyer", "Thomas Becker", "Lara Mendez",
]
POLICYHOLDER_PREFIXES = [
    "Aldermann", "Bridgeford", "Carrington", "Drummond",
    "Eastleigh", "Fenwick", "Glenshire", "Hartwell", "Inverness",
    "Jamieson", "Kingsley", "Langford", "Maitland", "Northwood",
]
POLICYHOLDER_SUFFIXES = [
    "Holdings Ltd", "Group plc", "Industries Inc", "Capital LLC",
    "Resources Ltd", "Partners LLP", "Technologies Inc",
]
JURISDICTIONS = [
    "Worldwide", "US excl. Worldwide", "Worldwide excl. US",
    "UK only", "EU only",
]
RATING_BASIS = ["Premium", "Limit", "Exposure"]
LIMIT_TYPES = ["Each and Every Loss", "Aggregate", "Each Claim"]
DEDUCTIBLE_TYPES = ["Each and Every Loss", "Aggregate"]
CLAIMS_TRIGGERS = ["Claims Made", "Occurrence", "Losses Discovered"]
EXPOSURE_TYPES = ["Turnover", "Wage Roll", "TIV", "Number of Insureds"]
PLATFORMS = ["LSM_Quoting_Platform", "Contingency_v2", "EU_PI_Product_v1"]
MODEL_NAMES = ["RM_v3.2", "RM_v3.3", "Contingency_v2", "EUPI_v1.4"]

# ---------------------------------------------------------------------------
# Build the quote dimension (single source of truth for keys & dimensions)
# ---------------------------------------------------------------------------

def build_quote_dim():
    """Return a list of dicts; one per quote. Keys + dim attributes only."""
    quotes = []
    base_inception = date(2025, 1, 1)
    for i in range(1, N_QUOTES + 1):
        section = random.choice(SECTIONS)
        is_renewal = random.random() < 0.70
        # Inception date spans -180d..+360d from base; bias toward recent
        offset_days = int(random.triangular(-180, 360, 90))
        inception = base_inception + timedelta(days=offset_days)
        expiry = inception + timedelta(days=365)
        quote_date = inception - timedelta(days=random.randint(15, 90))
        currency = random.choices(CURRENCIES, weights=[5, 5, 5, 2, 2])[0]
        broker = random.choice(BROKERS)
        underwriter = random.choice(UNDERWRITERS)
        policyholder = (
            f"{random.choice(POLICYHOLDER_PREFIXES)} "
            f"{random.choice(POLICYHOLDER_SUFFIXES)}"
        )

        # Subscription share (signed-line economics)
        our_share_pct = round(random.uniform(0.05, 0.30), 4)
        london_share_share = round(random.uniform(0.45, 0.85), 4)  # of our_share_pct that is London
        our_share_pct_london = round(our_share_pct * london_share_share, 4)
        our_share_pct_non_london = round(our_share_pct - our_share_pct_london, 4)
        london_signed_line = round(our_share_pct_london * random.uniform(0.85, 1.15), 4)
        london_order_pct = round(random.uniform(0.50, 0.95), 4)

        # Number of layers and coverages per quote
        n_layers = random.choices([1, 2, 3], weights=[0.55, 0.30, 0.15])[0]

        quotes.append({
            "quote_id":               f"Q{i:05d}",
            "pas_id":                 f"PAS-{i:06d}",
            "section":                section,
            "policyholder_name":      policyholder,
            "broker_primary":         broker,
            "underwriter":            underwriter,
            "premium_currency":       currency,
            "inception_date":         inception,
            "expiry_date":            expiry,
            "quote_date":             quote_date,
            "is_renewal":             is_renewal,
            "new_renewal":            "Renewal" if is_renewal else "New",
            "our_share_pct":          our_share_pct,
            "our_share_pct_london":   our_share_pct_london,
            "our_share_pct_non_london": our_share_pct_non_london,
            "london_signed_line":     london_signed_line,
            "london_order_pct":       london_order_pct,
            "n_layers":               n_layers,
            "platform":               random.choice(PLATFORMS),
            "model_name":             random.choice(MODEL_NAMES),
            "branch":                 random.choice(["London", "Dublin", "Madrid", "Singapore"]),
            "entity":                 "Liberty Specialty Markets",
            "rating_basis":           random.choice(RATING_BASIS),
        })
    return quotes


def build_layer_dim(quote_dim):
    """Return list of dicts at (quote_id, layer_id, pas_id) grain."""
    layers = []
    for q in quote_dim:
        n = q["n_layers"]
        for li in range(1, n + 1):
            # Technical premium per layer — lognormal around $200K, scaled per section
            section_scale = {
                "European Professional Indemnity": 1.0,
                "Directors and Officers": 1.4,
                "General Aviation": 2.0,
                "Cash in Transit and Specie": 0.6,
                "Contingency": 0.5,
            }[q["section"]]
            tech_gnwp_full = _lognormal(median=250_000 * section_scale, sigma=0.7)
            tech_gnwp_full = _clamp(tech_gnwp_full, 25_000, 8_000_000)
            # Modtech: sometimes uplift but usually discount
            modtech_factor = random.uniform(0.85, 1.10)
            modtech_gnwp_full = tech_gnwp_full * modtech_factor
            # Sold: typically below modtech (softening market)
            sold_factor = random.triangular(0.75, 1.10, 0.92)
            sold_gnwp_full = modtech_gnwp_full * sold_factor
            # Commission: 10-25% of sold premium
            commission_pct = random.uniform(0.10, 0.25)
            commission_full = sold_gnwp_full * commission_pct
            # ELC: target loss ratio band per quote
            target_lr = random.uniform(0.55, 0.78)
            tech_elc_full = tech_gnwp_full * target_lr
            modtech_elc_full = modtech_gnwp_full * (target_lr * random.uniform(0.95, 1.05))

            # GGWP measures (slightly higher than GNWP — gross of brokerage)
            tech_ggwp_full = tech_gnwp_full / (1 - commission_pct * 0.5)
            modtech_ggwp_full = modtech_gnwp_full / (1 - commission_pct * 0.5)
            sold_ggwp_full = sold_gnwp_full / (1 - commission_pct * 0.5)

            # Conditional premium reduction (rare flag)
            cpr_flag = random.choices(
                ["None", "Conditional discount applied", "Premium reduction conditional on no claims"],
                weights=[0.85, 0.10, 0.05],
            )[0]

            # Number of coverages on this layer
            n_cov = random.choices([1, 2], weights=[0.70, 0.30])[0]

            layers.append({
                "quote_id":               q["quote_id"],
                "pas_id":                 q["pas_id"],
                "layer_id":               f"{q['quote_id']}-L{li}",
                "layer_index":            li,
                "section":                q["section"],
                # Full-share measures (the "100%" view used by quote_policy_detail)
                "tech_gnwp_full":         tech_gnwp_full,
                "modtech_gnwp_full":      modtech_gnwp_full,
                "sold_gnwp_full":         sold_gnwp_full,
                "tech_ggwp_full":         tech_ggwp_full,
                "modtech_ggwp_full":      modtech_ggwp_full,
                "sold_ggwp_full":         sold_ggwp_full,
                "tech_elc_full":          tech_elc_full,
                "modtech_elc_full":       modtech_elc_full,
                "commission_full":        commission_full,
                "commission_pct":         commission_pct,
                "n_coverages":            n_cov,
                "conditional_premium_reduction": cpr_flag,
            })
    return layers


def build_coverage_dim(layers, quote_by_id):
    """Return list of dicts at (quote_id, layer_id, pas_id, coverage_id) grain."""
    coverages = []
    for L in layers:
        q = quote_by_id[L["quote_id"]]
        cov_options = COVERAGES_BY_SECTION[q["section"]]
        chosen_covs = random.sample(cov_options, k=min(L["n_coverages"], len(cov_options)))
        for ci, cov_name in enumerate(chosen_covs, start=1):
            primary = (ci == 1)
            # Exposure / limit / deductible / excess hierarchy
            exposure = _lognormal(median=5_000_000, sigma=1.0)
            exposure = _clamp(exposure, 100_000, 500_000_000)
            limit = exposure * random.uniform(0.05, 0.50)
            deductible = limit * random.uniform(0.001, 0.05)
            excess = deductible * random.uniform(1.0, 5.0)
            coverages.append({
                "quote_id":             L["quote_id"],
                "pas_id":                L["pas_id"],
                "layer_id":              L["layer_id"],
                "coverage_id":           f"{L['layer_id']}-C{ci}",
                "coverage_name":         cov_name,
                "primary_coverage":      primary,
                "exposure":              exposure,
                "exposure_type":         random.choice(EXPOSURE_TYPES),
                "limit":                 limit,
                "limit_type":            random.choice(LIMIT_TYPES),
                "excess":                excess,
                "deductible_value":      deductible,
                "deductible_type":       random.choice(DEDUCTIBLE_TYPES),
                "claims_trigger":        random.choice(CLAIMS_TRIGGERS),
                "policy_coverage_jurisdiction": random.choice(JURISDICTIONS),
                "subcoveragecode":       f"SC-{random.randint(100, 999)}",
            })
    return coverages

# ---------------------------------------------------------------------------
# Asset emitters
# ---------------------------------------------------------------------------

LL_QUOTE_SETUP_COLS = [
    "platform", "quote_id", "model_name", "last_updated_at",
    "_pdm_last_update_timestamp", "quote_date", "policyholder_name",
    "entity", "branch", "underwriter", "broker_primary",
    "inception_date", "expiry_date", "premium_currency", "policy_is_quoted",
]

def emit_ll_quote_setup(quote_dim):
    rows = []
    for q in quote_dim:
        rows.append({
            "platform":                  q["platform"],
            "quote_id":                  q["quote_id"],
            "model_name":                q["model_name"],
            "last_updated_at":           PDM_TS,
            "_pdm_last_update_timestamp": PDM_TS,
            "quote_date":                _date_iso(q["quote_date"]),
            "policyholder_name":         q["policyholder_name"],
            "entity":                    q["entity"],
            "branch":                    q["branch"],
            "underwriter":               q["underwriter"],
            "broker_primary":            q["broker_primary"],
            "inception_date":            _date_iso(q["inception_date"]),
            "expiry_date":               _date_iso(q["expiry_date"]),
            "premium_currency":          q["premium_currency"],
            "policy_is_quoted":          _bool_str(True),
        })
    _write_csv(OUT_DIR / "ll_quote_setup.csv", LL_QUOTE_SETUP_COLS, rows)
    return rows


LL_QUOTE_POLICY_DETAIL_COLS = [
    "layer_id", "quote_id", "pas_id", "_pdm_last_update_timestamp",
    "rating_basis", "conditional_premium_reduction",
    "our_share_pct", "our_share_pct_london", "our_share_pct_non_london",
    "london_estimated_signed_line", "london_order_percentage",
    "tech_gnwp", "modtech_gnwp", "sold_gnwp",
    "tech_ggwp", "modtech_ggwp", "sold_ggwp",
    "sold_ggwp_our_share",
    "tech_elc", "modtech_elc",
    "tech_gg_elr", "modtech_gg_elr", "tech_gn_elr", "modtech_gn_elr",
    "commission",
    "sold_to_modtech", "modtech_to_tech",
]

def emit_ll_quote_policy_detail(layers, quote_by_id):
    """Liberty share view: ll_* measures = full × our_share_pct (USD)."""
    rows = []
    for L in layers:
        q = quote_by_id[L["quote_id"]]
        sp = q["our_share_pct"]
        tech_gnwp = L["tech_gnwp_full"] * sp
        modtech_gnwp = L["modtech_gnwp_full"] * sp
        sold_gnwp = L["sold_gnwp_full"] * sp
        tech_ggwp = L["tech_ggwp_full"] * sp
        modtech_ggwp = L["modtech_ggwp_full"] * sp
        sold_ggwp = L["sold_ggwp_full"] * sp
        tech_elc = L["tech_elc_full"] * sp
        modtech_elc = L["modtech_elc_full"] * sp
        commission = L["commission_full"] * sp
        rows.append({
            "layer_id":                  L["layer_id"],
            "quote_id":                  L["quote_id"],
            "pas_id":                    L["pas_id"],
            "_pdm_last_update_timestamp": PDM_TS,
            "rating_basis":              q["rating_basis"],
            "conditional_premium_reduction": L["conditional_premium_reduction"],
            "our_share_pct":             q["our_share_pct"],
            "our_share_pct_london":      q["our_share_pct_london"],
            "our_share_pct_non_london":  q["our_share_pct_non_london"],
            "london_estimated_signed_line": q["london_signed_line"],
            "london_order_percentage":   q["london_order_pct"],
            "tech_gnwp":                 _round(tech_gnwp),
            "modtech_gnwp":              _round(modtech_gnwp),
            "sold_gnwp":                 _round(sold_gnwp),
            "tech_ggwp":                 _round(tech_ggwp),
            "modtech_ggwp":              _round(modtech_ggwp),
            "sold_ggwp":                 _round(sold_ggwp),
            "sold_ggwp_our_share":       _round(sold_ggwp),
            "tech_elc":                  _round(tech_elc),
            "modtech_elc":               _round(modtech_elc),
            "tech_gg_elr":               _round(tech_elc / tech_ggwp, 6) if tech_ggwp else None,
            "modtech_gg_elr":            _round(modtech_elc / modtech_ggwp, 6) if modtech_ggwp else None,
            "tech_gn_elr":               _round(tech_elc / tech_gnwp, 6) if tech_gnwp else None,
            "modtech_gn_elr":            _round(modtech_elc / modtech_gnwp, 6) if modtech_gnwp else None,
            "commission":                _round(commission),
            "sold_to_modtech":           _round(sold_gnwp / modtech_gnwp, 6) if modtech_gnwp else None,
            "modtech_to_tech":           _round(modtech_gnwp / tech_gnwp, 6) if tech_gnwp else None,
        })
    _write_csv(OUT_DIR / "ll_quote_policy_detail.csv", LL_QUOTE_POLICY_DETAIL_COLS, rows)
    return rows


QUOTE_POLICY_DETAIL_COLS = LL_QUOTE_POLICY_DETAIL_COLS  # same schema, full-share values

def emit_quote_policy_detail(layers, quote_by_id):
    """Full-share twin of ll_quote_policy_detail (100% market view)."""
    rows = []
    for L in layers:
        q = quote_by_id[L["quote_id"]]
        tech_gnwp = L["tech_gnwp_full"]
        modtech_gnwp = L["modtech_gnwp_full"]
        sold_gnwp = L["sold_gnwp_full"]
        tech_ggwp = L["tech_ggwp_full"]
        modtech_ggwp = L["modtech_ggwp_full"]
        sold_ggwp = L["sold_ggwp_full"]
        tech_elc = L["tech_elc_full"]
        modtech_elc = L["modtech_elc_full"]
        commission = L["commission_full"]
        rows.append({
            "layer_id":                  L["layer_id"],
            "quote_id":                  L["quote_id"],
            "pas_id":                    L["pas_id"],
            "_pdm_last_update_timestamp": PDM_TS,
            "rating_basis":              q["rating_basis"],
            "conditional_premium_reduction": L["conditional_premium_reduction"],
            "our_share_pct":             1.0,
            "our_share_pct_london":      1.0,
            "our_share_pct_non_london":  0.0,
            "london_estimated_signed_line": 1.0,
            "london_order_percentage":   q["london_order_pct"],
            "tech_gnwp":                 _round(tech_gnwp),
            "modtech_gnwp":              _round(modtech_gnwp),
            "sold_gnwp":                 _round(sold_gnwp),
            "tech_ggwp":                 _round(tech_ggwp),
            "modtech_ggwp":              _round(modtech_ggwp),
            "sold_ggwp":                 _round(sold_ggwp),
            "sold_ggwp_our_share":       _round(sold_ggwp * q["our_share_pct"]),
            "tech_elc":                  _round(tech_elc),
            "modtech_elc":               _round(modtech_elc),
            "tech_gg_elr":               _round(tech_elc / tech_ggwp, 6) if tech_ggwp else None,
            "modtech_gg_elr":            _round(modtech_elc / modtech_ggwp, 6) if modtech_ggwp else None,
            "tech_gn_elr":               _round(tech_elc / tech_gnwp, 6) if tech_gnwp else None,
            "modtech_gn_elr":            _round(modtech_elc / modtech_gnwp, 6) if modtech_gnwp else None,
            "commission":                _round(commission),
            "sold_to_modtech":           _round(sold_gnwp / modtech_gnwp, 6) if modtech_gnwp else None,
            "modtech_to_tech":           _round(modtech_gnwp / tech_gnwp, 6) if tech_gnwp else None,
        })
    _write_csv(OUT_DIR / "quote_policy_detail.csv", QUOTE_POLICY_DETAIL_COLS, rows)
    return rows


LL_QUOTE_COVERAGE_DETAIL_COLS = [
    "coverage_id", "layer_id", "quote_id", "pas_id",
    "_pdm_last_update_timestamp",
    "quote_name", "new_renewal", "section", "coverage", "subcoveragecode",
    "inception_date", "expiry_date",
    "exposure", "exposure_type", "limit", "limit_type",
    "excess", "deductible_value", "deductible_type",
    "claims_trigger", "policy_coverage_jurisdiction", "primary_coverage",
]

def emit_ll_quote_coverage_detail(coverages, quote_by_id):
    rows = []
    for c in coverages:
        q = quote_by_id[c["quote_id"]]
        rows.append({
            "coverage_id":               c["coverage_id"],
            "layer_id":                  c["layer_id"],
            "quote_id":                  c["quote_id"],
            "pas_id":                    c["pas_id"],
            "_pdm_last_update_timestamp": PDM_TS,
            "quote_name":                f"{q['policyholder_name']} {q['inception_date'].year}",
            "new_renewal":               q["new_renewal"],
            "section":                   q["section"],
            "coverage":                  c["coverage_name"],
            "subcoveragecode":           c["subcoveragecode"],
            "inception_date":            _date_iso(q["inception_date"]),
            "expiry_date":               _date_iso(q["expiry_date"]),
            "exposure":                  _round(c["exposure"]),
            "exposure_type":             c["exposure_type"],
            "limit":                     _round(c["limit"]),
            "limit_type":                c["limit_type"],
            "excess":                    _round(c["excess"]),
            "deductible_value":          _round(c["deductible_value"]),
            "deductible_type":           c["deductible_type"],
            "claims_trigger":            c["claims_trigger"],
            "policy_coverage_jurisdiction": c["policy_coverage_jurisdiction"],
            "primary_coverage":          _bool_str(c["primary_coverage"]),
        })
    _write_csv(OUT_DIR / "ll_quote_coverage_detail.csv", LL_QUOTE_COVERAGE_DETAIL_COLS, rows)
    return rows


RATE_MONITORING_LAYER_COLS = [
    "quote_id", "layer_id", "_pdm_last_update_timestamp",
    "expiring_inception_date", "expiring_expiry_date",
    "expiring_exposure", "expiring_limit", "expiring_excess", "expiring_deductible",
    "expiring_our_share_pct", "expiring_commission_percentage",
    "expiring_ggwp", "expiring_gnwp", "expiring_modtech_gnwp", "expiring_tech_gnwp",
    "expiring_as_if_ggwp",
    "gross_rarc", "net_rarc", "claims_inflation", "breadth_of_cover_change",
    "gross_exposure_change", "gross_limits_and_excess_change",
    "policy_term_change", "other_changes",
    "our_share_pct_london", "our_share_pct_non_london",
]

# Quote-grain seed inherits the same column set minus layer_id; ordering kept
# aligned with the previous seed contract for downstream stability.
RATE_MONITORING_COLS = [c for c in RATE_MONITORING_LAYER_COLS if c != "layer_id"]


def build_rate_monitoring_layer(quote_dim, layers_by_quote, coverages_by_layer):
    """Build the layer-grain rate-monitoring rows.

    One row per renewing (quote_id, layer_id). Each layer draws its own
    rate-change components independently so primary vs excess layers can
    move differently — which is the realistic invariant in a multi-layer
    specialty programme. Per-quote attributes (expiring inception/expiry,
    expiring_our_share_pct, london splits) are denormalised onto every
    layer row, mirroring the convention used in `ll_quote_policy_detail`.

    Returned in deterministic (quote_id, layer_index) order so subsequent
    aggregation produces a stable output.
    """
    rows = []
    for q in quote_dim:
        if not q["is_renewal"]:
            continue
        layers = layers_by_quote.get(q["quote_id"], [])
        if not layers:
            continue

        # Per-quote denormalised attributes.
        exp_inception = q["inception_date"] - timedelta(days=365)
        exp_expiry    = q["expiry_date"]    - timedelta(days=365)
        commission_pct_exp = random.uniform(0.10, 0.25)

        for L in sorted(layers, key=lambda L: L["layer_index"]):
            cov_rows = coverages_by_layer.get(L["layer_id"], [])

            # Layer expiring premium measures — layer's full-share figures
            # nudged by an independent YoY factor. Same convention as the
            # historical quote-grain emitter, applied per-layer.
            yoy_factor = random.uniform(0.85, 1.10)
            exp_tech_gnwp    = L["tech_gnwp_full"]    * yoy_factor
            exp_modtech_gnwp = L["modtech_gnwp_full"] * yoy_factor
            exp_gnwp         = L["sold_gnwp_full"]    * yoy_factor
            exp_ggwp         = exp_gnwp / (1 - 0.10)

            # Layer expiring structural measures from its own coverages.
            if cov_rows:
                exp_exposure   = sum(c["exposure"]         for c in cov_rows) * random.uniform(0.85, 1.10)
                exp_limit      = sum(c["limit"]            for c in cov_rows) * random.uniform(0.85, 1.10)
                exp_excess     = (sum(c["excess"]          for c in cov_rows) / len(cov_rows)) * random.uniform(0.85, 1.10)
                exp_deductible = (sum(c["deductible_value"] for c in cov_rows) / len(cov_rows)) * random.uniform(0.85, 1.10)
            else:
                exp_exposure = exp_limit = exp_excess = exp_deductible = 0.0

            # Independent layer-level rate-change components.
            gross_rarc       = random.triangular(-0.15, 0.15, -0.02)
            claims_inflation = random.uniform(0.02, 0.08)
            breadth          = random.uniform(-0.05, 0.05)
            exposure_change  = random.uniform(-0.10, 0.15)
            limits_change    = random.uniform(-0.05, 0.10)
            term_change      = random.uniform(-0.05, 0.05)
            other_changes    = random.uniform(-0.02, 0.02)
            net_rarc         = gross_rarc - claims_inflation - breadth + random.uniform(-0.01, 0.01)

            rows.append({
                "quote_id":                  q["quote_id"],
                "layer_id":                  L["layer_id"],
                "_pdm_last_update_timestamp": PDM_TS,
                "expiring_inception_date":   _date_iso(exp_inception),
                "expiring_expiry_date":      _date_iso(exp_expiry),
                "expiring_exposure":         _round(exp_exposure),
                "expiring_limit":            _round(exp_limit),
                "expiring_excess":           _round(exp_excess),
                "expiring_deductible":       _round(exp_deductible),
                "expiring_our_share_pct":    q["our_share_pct"],
                "expiring_commission_percentage": _round(commission_pct_exp, 4),
                "expiring_ggwp":             _round(exp_ggwp),
                "expiring_gnwp":             _round(exp_gnwp),
                "expiring_modtech_gnwp":     _round(exp_modtech_gnwp),
                "expiring_tech_gnwp":        _round(exp_tech_gnwp),
                "expiring_as_if_ggwp":       _round(exp_ggwp * random.uniform(0.95, 1.05)),
                "gross_rarc":                _round(gross_rarc, 6),
                "net_rarc":                  _round(net_rarc, 6),
                "claims_inflation":          _round(claims_inflation, 6),
                "breadth_of_cover_change":   _round(breadth, 6),
                "gross_exposure_change":     _round(exposure_change, 6),
                "gross_limits_and_excess_change": _round(limits_change, 6),
                "policy_term_change":        _round(term_change, 6),
                "other_changes":             _round(other_changes, 6),
                "our_share_pct_london":      q["our_share_pct_london"],
                "our_share_pct_non_london":  q["our_share_pct_non_london"],
            })
    return rows


def emit_rate_monitoring_layer(layer_rate_rows):
    _write_csv(OUT_DIR / "rate_monitoring.csv", RATE_MONITORING_LAYER_COLS, layer_rate_rows)
    return layer_rate_rows


def emit_rate_monitoring_quote(layer_rate_rows, layer_by_id):
    """Aggregate the layer-grain rate rows up to quote grain.

    Reconciliation invariant
    ------------------------
    The quote-grain figures are produced *exclusively* by aggregating the
    layer-grain rows. A consumer who premium-weights the layer rows back up
    will exactly match the quote-grain seed.

      • Premium measures (expiring_*_gnwp / _ggwp / _as_if_ggwp / _exposure /
        _limit) → SUM across layers.
      • Rate-change components (gross_rarc, net_rarc, claims_inflation, the
        five drivers) → premium-weighted average using the layer's
        `tech_gnwp_full` as the weight (so a heavily-priced primary layer
        dominates the quote-level rarc).
      • Per-layer averages (expiring_excess, expiring_deductible) → simple
        mean across layers, matching the historical generator's convention.
      • Per-quote attributes (expiring inception/expiry, share %, london
        split, commission_pct) → carried from the first layer, since they
        are denormalised identical values.
    """
    # Group layer rows by quote_id while preserving insertion order.
    by_quote: dict[str, list[dict]] = {}
    for r in layer_rate_rows:
        by_quote.setdefault(r["quote_id"], []).append(r)

    def w_avg(records, weights, field):
        total_w = sum(weights) or 1.0
        return sum(rec[field] * w for rec, w in zip(records, weights)) / total_w

    rows = []
    for quote_id, layer_rows in by_quote.items():
        weights = [layer_by_id[r["layer_id"]]["tech_gnwp_full"] for r in layer_rows]
        head = layer_rows[0]
        n_layers = len(layer_rows)

        rows.append({
            "quote_id":                  quote_id,
            "_pdm_last_update_timestamp": PDM_TS,
            "expiring_inception_date":   head["expiring_inception_date"],
            "expiring_expiry_date":      head["expiring_expiry_date"],
            # Sums across layers for additive structural measures
            "expiring_exposure":         _round(sum(r["expiring_exposure"] for r in layer_rows)),
            "expiring_limit":            _round(sum(r["expiring_limit"]    for r in layer_rows)),
            # Means for non-additive structural measures
            "expiring_excess":           _round(sum(r["expiring_excess"]      for r in layer_rows) / n_layers),
            "expiring_deductible":       _round(sum(r["expiring_deductible"]  for r in layer_rows) / n_layers),
            # Per-quote denormalised attributes
            "expiring_our_share_pct":    head["expiring_our_share_pct"],
            "expiring_commission_percentage": head["expiring_commission_percentage"],
            # Sums across layers for premium measures
            "expiring_ggwp":             _round(sum(r["expiring_ggwp"]         for r in layer_rows)),
            "expiring_gnwp":             _round(sum(r["expiring_gnwp"]         for r in layer_rows)),
            "expiring_modtech_gnwp":     _round(sum(r["expiring_modtech_gnwp"] for r in layer_rows)),
            "expiring_tech_gnwp":        _round(sum(r["expiring_tech_gnwp"]    for r in layer_rows)),
            "expiring_as_if_ggwp":       _round(sum(r["expiring_as_if_ggwp"]   for r in layer_rows)),
            # Premium-weighted averages for rate-change components
            "gross_rarc":                _round(w_avg(layer_rows, weights, "gross_rarc"),       6),
            "net_rarc":                  _round(w_avg(layer_rows, weights, "net_rarc"),         6),
            "claims_inflation":          _round(w_avg(layer_rows, weights, "claims_inflation"), 6),
            "breadth_of_cover_change":   _round(w_avg(layer_rows, weights, "breadth_of_cover_change"), 6),
            "gross_exposure_change":     _round(w_avg(layer_rows, weights, "gross_exposure_change"),   6),
            "gross_limits_and_excess_change": _round(w_avg(layer_rows, weights, "gross_limits_and_excess_change"), 6),
            "policy_term_change":        _round(w_avg(layer_rows, weights, "policy_term_change"), 6),
            "other_changes":             _round(w_avg(layer_rows, weights, "other_changes"),     6),
            # Per-quote denormalised
            "our_share_pct_london":      head["our_share_pct_london"],
            "our_share_pct_non_london":  head["our_share_pct_non_london"],
        })
    _write_csv(OUT_DIR / "rate_monitoring_total_our_share_usd.csv", RATE_MONITORING_COLS, rows)
    return rows

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    print(f"Generating Pack B mock data → {OUT_DIR}")

    quote_dim = build_quote_dim()
    quote_by_id = {q["quote_id"]: q for q in quote_dim}
    layers = build_layer_dim(quote_dim)
    layer_by_id = {L["layer_id"]: L for L in layers}
    coverages = build_coverage_dim(layers, quote_by_id)

    layers_by_quote: dict[str, list[dict]] = {}
    for L in layers:
        layers_by_quote.setdefault(L["quote_id"], []).append(L)
    coverages_by_layer: dict[str, list[dict]] = {}
    for c in coverages:
        coverages_by_layer.setdefault(c["layer_id"], []).append(c)

    setup_rows    = emit_ll_quote_setup(quote_dim)
    ll_pol_rows   = emit_ll_quote_policy_detail(layers, quote_by_id)
    full_pol_rows = emit_quote_policy_detail(layers, quote_by_id)
    ll_cov_rows   = emit_ll_quote_coverage_detail(coverages, quote_by_id)

    # Layer-grain first; quote-grain seed is derived from it by premium-
    # weighted aggregation, so the two reconcile by construction.
    rm_layer_rows = build_rate_monitoring_layer(quote_dim, layers_by_quote, coverages_by_layer)
    emit_rate_monitoring_layer(rm_layer_rows)
    rm_quote_rows = emit_rate_monitoring_quote(rm_layer_rows, layer_by_id)

    print(f"  ll_quote_setup.csv                       {len(setup_rows):5d} rows")
    print(f"  ll_quote_policy_detail.csv               {len(ll_pol_rows):5d} rows")
    print(f"  quote_policy_detail.csv                  {len(full_pol_rows):5d} rows")
    print(f"  ll_quote_coverage_detail.csv             {len(ll_cov_rows):5d} rows")
    print(f"  rate_monitoring.csv                      {len(rm_layer_rows):5d} rows  (layer grain, renewals only)")
    print(f"  rate_monitoring_total_our_share_usd.csv  {len(rm_quote_rows):5d} rows  (quote grain, renewals only)")
    print(f"\nKey cardinality:  {len(quote_dim)} quotes, {len(layers)} layers, {len(coverages)} coverages")


if __name__ == "__main__":
    main()
