#!/usr/bin/env python
"""Generate synthetic mock data for pricing_adequacy_monitoring.

Produces:
  output/mock_data/pricing_adequacy_monitoring.csv          — 300 quotes
  output/mock_data/pricing_adequacy_monitoring_at_risk.csv  — gross_rarc < 0, sorted worst-first

Columns match the data_requisite for pricing_adequacy_monitoring:
  Identifier : quote_id
  Dimensions : policyholder_name, underwriter, broker_primary, premium_currency
               (from ll_quote_setup via join on quote_id)
  Measures   : claims_inflation, gross_rarc, net_rarc, breadth_of_cover_change,
               expiring_exposure, expiring_limit, expiring_excess, expiring_deductible
  Time       : expiring_inception_date, expiring_expiry_date
  Other      : new_renewal, section, expiring_policy_currency, expiring_gnwp,
               expiring_our_share_pct, expiring_commission_percentage,
               gross_exposure_change, gross_limits_and_excess_change, policy_term_change

Run with:
  python scripts/generate_mock_data.py
"""

import csv
import math
import random
import sys
from datetime import date, timedelta
from pathlib import Path

# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------
SEED = 42
random.seed(SEED)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _round_to(value: float, nearest: float) -> float:
    return round(round(value / nearest) * nearest, 10)


def _clamp(value: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, value))


def _lognormal(median: float, sigma: float) -> float:
    """Log-normal sample with given median and shape parameter sigma."""
    mu = math.log(median)
    u1 = random.random() or 1e-12
    u2 = random.random() or 1e-12
    z = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
    return math.exp(mu + sigma * z)


def _normal(mean: float, std: float) -> float:
    """Normal sample using Box-Muller."""
    u1 = random.random() or 1e-12
    u2 = random.random() or 1e-12
    z = math.sqrt(-2 * math.log(u1)) * math.cos(2 * math.pi * u2)
    return mean + std * z


def _weighted_choice(choices: list, weights: list):
    """Choose from choices according to weights (must sum to 1.0)."""
    r = random.random()
    cumulative = 0.0
    for choice, w in zip(choices, weights):
        cumulative += w
        if r <= cumulative:
            return choice
    return choices[-1]


def _months_between(d1: date, d2: date) -> int:
    return (d2.year - d1.year) * 12 + (d2.month - d1.month)


# ---------------------------------------------------------------------------
# Date generation
# ---------------------------------------------------------------------------

START_DATE = date(2022, 1, 1)
END_DATE   = date(2024, 1, 1)
_TOTAL_MONTHS = _months_between(START_DATE, END_DATE)  # 24 months


def _random_inception_date() -> date:
    """Uniform distribution, rounded to first of month."""
    month_offset = random.randint(0, _TOTAL_MONTHS - 1)
    year  = START_DATE.year + (START_DATE.month - 1 + month_offset) // 12
    month = (START_DATE.month - 1 + month_offset) % 12 + 1
    return date(year, month, 1)


# ---------------------------------------------------------------------------
# Distribution parameters
# ---------------------------------------------------------------------------

SECTIONS = [
    "Professional Indemnity",
    "Directors & Officers",
    "General Aviation",
    "Contingency",
    "Cash in Transit & Specie",
]
SECTION_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]

# gross_rarc segment boundaries
RARC_BELOW = (-0.08,  0.00)   # 30% at risk
RARC_AT    = ( 0.00,  0.08)   # 45% adequate
RARC_ABOVE = ( 0.08,  0.20)   # 25% strong

# Section RARC bias (mean shift applied before clamping)
SECTION_RARC_BIAS = {
    "Professional Indemnity": 0.00,
    "Directors & Officers":  -0.02,
    "General Aviation":      +0.02,
    "Contingency":           -0.01,
    "Cash in Transit & Specie": 0.00,
}

# ---------------------------------------------------------------------------
# Dimension pools (ll_quote_setup)
# ---------------------------------------------------------------------------

# 80 policyholder names — realistic specialty insurance clients
_POLICYHOLDERS = [
    # Financial institutions
    "Meridian Capital Bank plc", "Fortress Asset Management Ltd",
    "Apex Financial Group Inc", "Sterling Private Equity SA",
    "Harrington Investment Partners Ltd", "Blackwater Fund Management plc",
    "Summit Wealth Advisors GmbH", "Cascade Credit Solutions Ltd",
    "Vantage Capital Partners Inc", "Pinnacle Asset Management plc",
    "Redwood Financial Services Ltd", "Clearwater Banking Group SA",
    "Cardinal Trust & Investment Ltd", "Ironbridge Capital Inc",
    "Westgate Securities plc", "Northbrook Fund Services GmbH",
    "Silverstone Asset Finance Ltd", "Broadmoor Capital SA",
    "Greystone Wealth Management Inc", "Thornbury Banking Corp plc",
    # Aviation companies
    "Pacific Air Holdings Inc", "Atlas Aviation GmbH",
    "Sovereign Flight Operations Ltd", "Meridian Cargo Airlines SA",
    "Skybridge Charter Services plc", "Continental Air Freight Inc",
    "Horizon Aviation Group Ltd", "Pinnacle Helicopter Services GmbH",
    "Southern Cross Airlines Inc", "Starpoint Air Ltd",
    "Nordic Aviation Solutions SA", "Coastal Air Charters plc",
    "Transpacific Cargo Inc", "Highland Rotorcraft Ltd",
    "Oceanic Air Freight GmbH", "Midland Aviation Services plc",
    # Contingency event organizers
    "Global Events Group Ltd", "Premier Entertainment SA",
    "Marquee Events International Inc", "Centennial Festivals plc",
    "Prestige Sporting Events Ltd", "Grandstand Events GmbH",
    "Colosseum Entertainment Inc", "Ovation Live Events SA",
    "Pinnacle Concerts Ltd", "Horizon Festivals plc",
    "Stellar Productions Inc", "Crown Events Management GmbH",
    "Apex Sports Events Ltd", "Landmark Exhibitions SA",
    "Cobalt Entertainment Inc", "Summit Sporting Productions plc",
    # Professional services
    "Stanhope & Partners LLP", "Wellington Advisory Ltd",
    "Pemberton Consulting Group Inc", "Arden Legal Services plc",
    "Chester & Whitmore LLP", "Oakfield Professional Services Ltd",
    "Granville Accountancy Group SA", "Hartley & Associates Inc",
    "Montague Actuarial Services plc", "Clifton Risk Consulting Ltd",
    "Fairbridge Management Consulting GmbH", "Royston & Carr LLP",
    "Elsworth Technology Group Ltd", "Aldgate Professional Services Inc",
    "Buckingham Advisory plc", "Cavendish Risk Solutions Ltd",
    "Drayton Governance Partners SA", "Elgar Compliance Group Inc",
    "Foxhall Audit & Assurance plc", "Greycoat Technology Services Ltd",
    # Mixed / other specialty
    "Harrow Healthcare Group plc", "Islington Media Holdings Inc",
    "Juniper Pharma Research Ltd", "Kensington Retail Group SA",
    "Lambeth Property Holdings GmbH", "Moorgate Infrastructure plc",
    "Norbury Shipping & Logistics Ltd", "Oswald Mining Corp Inc",
    "Paddington Rail Holdings SA", "Queenhithe Energy Group plc",
]

# 12 underwriters — top 3 handle ~50% of book
_UNDERWRITERS = [
    "James Whitmore",     # senior
    "Sarah Blackwood",    # senior
    "David Harrington",   # senior
    "Emma Caldwell",
    "Michael Stanton",
    "Olivia Pemberton",
    "Robert Ashford",
    "Charlotte Henley",
    "Thomas Kirkland",
    "Amelia Forsythe",
    "Nicholas Wren",
    "Laura Thornton",
]
# Top 3 share ~50%; remaining 9 share ~50%
_UW_WEIGHTS = [0.20, 0.17, 0.13] + [round(0.50 / 9, 4)] * 9
# Normalise to sum exactly 1.0
_uw_sum = sum(_UW_WEIGHTS)
_UW_WEIGHTS = [w / _uw_sum for w in _UW_WEIGHTS]

# 10 Lloyd's brokers with specified weights
_BROKERS = [
    "Marsh",
    "Aon",
    "Willis Towers Watson",
    "Gallagher",
    "Lockton",
    "BMS Group",
    "McGill and Partners",
    "Howden",
    "Tysers",
    "Ed Broking",
]
# Remaining 6 split 35% equally
_remaining_broker_weight = 0.35 / 6
_BROKER_WEIGHTS = [0.20, 0.18, 0.15, 0.12,
                   _remaining_broker_weight, _remaining_broker_weight,
                   _remaining_broker_weight, _remaining_broker_weight,
                   _remaining_broker_weight, _remaining_broker_weight]
_bw_sum = sum(_BROKER_WEIGHTS)
_BROKER_WEIGHTS = [w / _bw_sum for w in _BROKER_WEIGHTS]


def _pick_premium_currency(policy_currency: str) -> str:
    """premium_currency correlated with expiring_policy_currency."""
    if policy_currency == "USD":
        return _weighted_choice(["USD", "GBP", "EUR"], [0.90, 0.06, 0.04])
    elif policy_currency == "GBP":
        return _weighted_choice(["GBP", "USD", "EUR"], [0.88, 0.08, 0.04])
    else:  # EUR
        return _weighted_choice(["EUR", "USD", "GBP"], [0.85, 0.10, 0.05])


# ---------------------------------------------------------------------------
# Row generation
# ---------------------------------------------------------------------------

def _generate_row_with_inception(inception: date, seq: int) -> dict:
    """Generate a row using a pre-determined inception date and sequence number."""
    year_str = str(inception.year)
    quote_id = f"Q-{year_str}-{seq:04d}"

    expiry = date(inception.year + 1, inception.month, 1)

    new_renewal = _weighted_choice(["Renewal", "New"], [0.75, 0.25])
    section     = _weighted_choice(SECTIONS, SECTION_WEIGHTS)
    currency    = _weighted_choice(["USD", "GBP", "EUR"], [0.60, 0.25, 0.15])

    # Dimension columns from ll_quote_setup (joined on quote_id)
    policyholder_name = random.choice(_POLICYHOLDERS)
    underwriter       = _weighted_choice(_UNDERWRITERS, _UW_WEIGHTS)
    broker_primary    = _weighted_choice(_BROKERS, _BROKER_WEIGHTS)
    premium_currency  = _pick_premium_currency(currency)

    raw_exposure = _lognormal(2_000_000, 1.0)
    exposure = _round_to(_clamp(raw_exposure, 100_000, 50_000_000), 10_000)

    limit_mult = random.uniform(2.0, 10.0)
    limit = _round_to(_clamp(exposure * limit_mult, 500_000, 200_000_000), 100_000)

    if random.random() < 0.20:
        excess = 0.0
    else:
        excess = _round_to(_clamp(random.uniform(0.0, 0.20) * limit, 0, limit * 0.20), 10_000)

    if random.random() < 0.30:
        deductible = 0.0
    else:
        deductible = _round_to(_clamp(random.uniform(0.0, 0.05) * exposure, 0, exposure * 0.05), 5_000)

    gnwp = _round_to(_clamp(exposure * random.uniform(0.015, 0.040), 10_000, 2_000_000), 1_000)

    our_share  = round(_clamp(_normal(0.22, 0.08), 0.05, 0.50), 2)
    commission = round(_clamp(_normal(0.135, 0.035), 0.05, 0.25), 3)
    claims_inf = round(_clamp(_normal(0.055, 0.012), 0.030, 0.090), 3)
    breadth    = round(_clamp(_normal(0.01, 0.05), -0.15, 0.15), 3)
    gross_exp_chg = round(_clamp(_normal(0.05, 0.09), -0.20, 0.30), 3)
    lx_chg     = round(_clamp(_normal(0.0, 0.04), -0.10, 0.10), 3)
    ptc = round(_clamp(_normal(0.0, 0.025), -0.05, 0.05), 3) if random.random() < 0.10 else 0.0

    bias = SECTION_RARC_BIAS.get(section, 0.0)
    seg_roll = random.random()
    if seg_roll < 0.30:
        lo, hi = RARC_BELOW
    elif seg_roll < 0.75:
        lo, hi = RARC_AT
    else:
        lo, hi = RARC_ABOVE
    gross_rarc = round(_clamp(_normal((lo + hi) / 2.0 + bias, (hi - lo) / 4.0), lo - 0.02, hi + 0.02), 3)

    net_adj  = commission * 0.3 + claims_inf * 0.2 + random.uniform(-0.01, 0.01)
    net_rarc = round(gross_rarc - net_adj, 3)

    return {
        # Identifier
        "quote_id":                       quote_id,
        # Dimensions from ll_quote_setup (join on quote_id)
        "policyholder_name":              policyholder_name,
        "underwriter":                    underwriter,
        "broker_primary":                 broker_primary,
        "premium_currency":               premium_currency,
        # Time
        "expiring_inception_date":        inception.isoformat(),
        "expiring_expiry_date":           expiry.isoformat(),
        # Categorical
        "new_renewal":                    new_renewal,
        "section":                        section,
        "expiring_policy_currency":       currency,
        # Measures
        "expiring_exposure":              exposure,
        "expiring_limit":                 limit,
        "expiring_excess":                excess,
        "expiring_deductible":            deductible,
        "expiring_gnwp":                  gnwp,
        "expiring_our_share_pct":         our_share,
        "expiring_commission_percentage": commission,
        "claims_inflation":               claims_inf,
        "breadth_of_cover_change":        breadth,
        "gross_exposure_change":          gross_exp_chg,
        "gross_limits_and_excess_change": lx_chg,
        "policy_term_change":             ptc,
        "gross_rarc":                     gross_rarc,
        "net_rarc":                       net_rarc,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    out_dir = Path("output/mock_data")
    out_dir.mkdir(parents=True, exist_ok=True)

    n_rows = 300

    year_counters: dict = {}
    rows = []
    for _ in range(n_rows):
        inception = _random_inception_date()
        y = inception.year
        year_counters[y] = year_counters.get(y, 0) + 1
        seq = year_counters[y]
        row = _generate_row_with_inception(inception, seq)
        rows.append(row)

    # --- Write main CSV ---
    main_path = out_dir / "pricing_adequacy_monitoring.csv"
    fieldnames = list(rows[0].keys())
    with main_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # --- Write at-risk CSV ---
    at_risk = sorted(
        [r for r in rows if r["gross_rarc"] < 0],
        key=lambda r: r["gross_rarc"],
    )
    at_risk_path = out_dir / "pricing_adequacy_monitoring_at_risk.csv"
    with at_risk_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(at_risk)

    # --- Summary ---
    below  = [r for r in rows if r["gross_rarc"] < 0]
    at_par = [r for r in rows if 0 <= r["gross_rarc"] < 0.05]
    above  = [r for r in rows if r["gross_rarc"] >= 0.05]

    print(f"Total quotes: {len(rows)}")
    print(f"Below technical price (gross_rarc < 0):      {len(below):3d} ({len(below)/len(rows)*100:.0f}%)")
    print(f"At technical price (0 <= gross_rarc < 0.05): {len(at_par):3d} ({len(at_par)/len(rows)*100:.0f}%)")
    print(f"Above technical price (gross_rarc >= 0.05):  {len(above):3d} ({len(above)/len(rows)*100:.0f}%)")
    print()

    print("By section — mean gross_rarc:")
    section_sums: dict  = {}
    section_counts: dict = {}
    for r in rows:
        s = r["section"]
        section_sums[s]   = section_sums.get(s, 0.0)   + r["gross_rarc"]
        section_counts[s] = section_counts.get(s, 0)   + 1
    print(f"  {'Section':<30s}  {'Count':>5}  {'Mean RARC':>10}")
    print(f"  {'-'*30}  {'-----':>5}  {'---------':>10}")
    for section in SECTIONS:
        cnt  = section_counts.get(section, 0)
        mean = section_sums.get(section, 0.0) / cnt if cnt else 0.0
        print(f"  {section:<30s}  {cnt:>5}  {mean:>+10.3f}")

    print()
    print("By broker — quote count:")
    broker_counts: dict = {}
    for r in rows:
        b = r["broker_primary"]
        broker_counts[b] = broker_counts.get(b, 0) + 1
    for broker in _BROKERS:
        cnt = broker_counts.get(broker, 0)
        print(f"  {broker:<25s}  {cnt:>4}  ({cnt/len(rows)*100:.0f}%)")

    print()
    print(f"Saved: {main_path}  ({len(rows)} rows, {len(fieldnames)} columns)")
    print(f"Saved: {at_risk_path}  ({len(at_risk)} rows, gross_rarc < 0, sorted worst-first)")


if __name__ == "__main__":
    main()
