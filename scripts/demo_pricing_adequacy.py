"""Pricing Adequacy Monitor — Streamlit demo.

Data product: pricing_adequacy_monitoring_dashboard
Fact table:   rate_monitoring_total_our_share_usd
Dimension:    ll_quote_setup (joined on quote_id)
Grain:        one row per quote

Run with:
  uv run streamlit run scripts/demo_pricing_adequacy.py
"""

from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Pricing Adequacy Monitor",
    page_icon="📊",
    layout="wide",
)

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
_DATA_PATH = Path(__file__).resolve().parent.parent / "output" / "mock_data" / "pricing_adequacy_monitoring.csv"


@st.cache_data
def load_data() -> pd.DataFrame:
    df = pd.read_csv(_DATA_PATH, parse_dates=["expiring_inception_date", "expiring_expiry_date"])
    return df


df_all = load_data()

# ---------------------------------------------------------------------------
# Zone colour palette
# ---------------------------------------------------------------------------
_RED   = "#E24B4A"
_AMBER = "#BA7517"
_TEAL  = "#1D9E75"


def _zone_color(rarc: float, threshold: float) -> str:
    if rarc < threshold:
        return _RED
    if rarc < 0.05:
        return _AMBER
    return _TEAL


def _assign_zones(series: pd.Series, threshold: float) -> pd.Series:
    return series.apply(lambda v: (
        "Below threshold" if v < threshold
        else ("0–5%" if v < 0.05 else "Above 5%")
    ))


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("## Pricing Adequacy Monitor")
    st.markdown("Real-time portfolio monitoring · Liberty Specialty Markets")
    st.markdown("---")

    threshold_pct = st.slider(
        "Flag renewals below:",
        min_value=-15.0,
        max_value=15.0,
        value=0.0,
        step=0.5,
        format="%.1f%%",
        help="Gross RARC threshold — renewals below this value appear in the Action list",
    )
    threshold = threshold_pct / 100

    sections_all = sorted(df_all["section"].unique())
    selected_sections = st.multiselect("Section", options=sections_all, default=sections_all)

    brokers_all = sorted(df_all["broker_primary"].unique())
    selected_brokers = st.multiselect("Broker", options=brokers_all, default=brokers_all)

    nr_filter = st.radio("New / Renewal", options=["All", "New", "Renewal"], index=0)

    underwriters_all = sorted(df_all["underwriter"].unique())
    selected_uw = st.multiselect("Underwriter", options=underwriters_all, default=underwriters_all)

    st.markdown("---")
    st.caption("303 tests · 19 initiatives · powered by AI")
    st.caption("Source: rate_monitoring_total_our_share_usd")
    st.caption("Grain: one row per quote")

# ---------------------------------------------------------------------------
# Apply filters
# ---------------------------------------------------------------------------
df = df_all.copy()
if selected_sections:
    df = df[df["section"].isin(selected_sections)]
if selected_brokers:
    df = df[df["broker_primary"].isin(selected_brokers)]
if nr_filter != "All":
    df = df[df["new_renewal"] == nr_filter]
if selected_uw:
    df = df[df["underwriter"].isin(selected_uw)]

n = len(df)

# ---------------------------------------------------------------------------
# Tabs
# ---------------------------------------------------------------------------
tab1, tab2, tab3 = st.tabs(["Portfolio overview", "Action list", "RARC decomposition"])

# ============================================================
# TAB 1 — Portfolio overview
# ============================================================
with tab1:
    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    total_gwp = df["expiring_gnwp"].sum()
    below_threshold = (df["gross_rarc"] < threshold).sum()
    pct_below = below_threshold / n * 100 if n else 0
    avg_rarc = df["gross_rarc"].mean() if n else 0.0

    col1.metric("Quotes", f"{n:,}")
    col2.metric("Expiring GWP", f"${total_gwp/1e6:.1f}m")
    col3.metric(
        "Below technical",
        f"{below_threshold} ({pct_below:.0f}%)",
        delta=f"-{pct_below:.0f}% of portfolio",
        delta_color="inverse",
    )
    rarc_delta = f"{avg_rarc:+.1%}"
    col4.metric(
        "Avg gross RARC",
        f"{avg_rarc:+.1%}",
        delta=rarc_delta,
        delta_color="normal" if avg_rarc >= 0 else "inverse",
    )

    st.markdown("")

    # ── RARC distribution histogram ──────────────────────────────────────
    if n > 0:
        df_hist = df.copy()
        df_hist["zone"] = _assign_zones(df_hist["gross_rarc"], threshold)

        zone_order = ["Below threshold", "0–5%", "Above 5%"]
        color_map = {
            "Below threshold": _RED,
            "0–5%":            _AMBER,
            "Above 5%":        _TEAL,
        }

        fig_hist = px.histogram(
            df_hist,
            x="gross_rarc",
            color="zone",
            color_discrete_map=color_map,
            category_orders={"zone": zone_order},
            nbins=40,
            title=f"RARC distribution — {n} quotes",
            labels={"gross_rarc": "Gross RARC", "zone": "Zone"},
            height=320,
        )
        fig_hist.add_vline(x=0, line_dash="dash", line_color="white",
                           annotation_text="Technical price",
                           annotation_position="top right")
        if threshold != 0:
            fig_hist.add_vline(x=threshold, line_dash="dot", line_color="#aaa",
                               annotation_text="Threshold",
                               annotation_position="top left")
        fig_hist.update_layout(
            paper_bgcolor="rgba(0,0,0,0)",
            plot_bgcolor="rgba(0,0,0,0)",
            showlegend=True,
            xaxis=dict(showgrid=False, tickformat=".0%"),
            yaxis=dict(showgrid=False),
            bargap=0.05,
        )
        st.plotly_chart(fig_hist, use_container_width=True)

    col_l, col_r = st.columns(2)

    # ── RARC by section ───────────────────────────────────────────────────
    with col_l:
        if n > 0:
            section_means = (
                df.groupby("section")["gross_rarc"]
                .mean()
                .reset_index()
                .sort_values("gross_rarc")
            )
            section_means["zone"] = _assign_zones(section_means["gross_rarc"], threshold)

            fig_sec = px.bar(
                section_means,
                x="section",
                y="gross_rarc",
                color="zone",
                color_discrete_map={
                    "Below threshold": _RED,
                    "0–5%": _AMBER,
                    "Above 5%": _TEAL,
                },
                title="Average RARC by class of business",
                labels={"gross_rarc": "Mean gross RARC", "section": ""},
                height=280,
            )
            fig_sec.add_hline(y=0, line_dash="dash", line_color="white")
            if threshold != 0:
                fig_sec.add_hline(y=threshold, line_dash="dot", line_color="#aaa")
            fig_sec.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False, tickformat=".0%"),
            )
            st.plotly_chart(fig_sec, use_container_width=True)

    # ── RARC by broker ────────────────────────────────────────────────────
    with col_r:
        if n > 0:
            broker_means = (
                df.groupby("broker_primary")["gross_rarc"]
                .mean()
                .reset_index()
                .sort_values("gross_rarc")
            )
            broker_means["zone"] = _assign_zones(broker_means["gross_rarc"], threshold)

            fig_brk = px.bar(
                broker_means,
                x="broker_primary",
                y="gross_rarc",
                color="zone",
                color_discrete_map={
                    "Below threshold": _RED,
                    "0–5%": _AMBER,
                    "Above 5%": _TEAL,
                },
                title="Average RARC by broker",
                labels={"gross_rarc": "Mean gross RARC", "broker_primary": ""},
                height=280,
            )
            fig_brk.add_hline(y=0, line_dash="dash", line_color="white")
            if threshold != 0:
                fig_brk.add_hline(y=threshold, line_dash="dot", line_color="#aaa")
            fig_brk.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                showlegend=False,
                xaxis=dict(showgrid=False, tickangle=45),
                yaxis=dict(showgrid=False, tickformat=".0%"),
            )
            st.plotly_chart(fig_brk, use_container_width=True)


# ============================================================
# TAB 2 — Action list
# ============================================================
with tab2:
    df_flagged = df[df["gross_rarc"] < threshold].sort_values("gross_rarc")
    n_flagged = len(df_flagged)
    at_risk_gwp = df_flagged["expiring_gnwp"].sum()
    pct_at_risk = at_risk_gwp / total_gwp * 100 if total_gwp else 0

    st.subheader(f"Renewals below {threshold:.1%} threshold")
    st.caption(
        f"{n_flagged} quotes flagged · "
        f"${at_risk_gwp:,.0f} at-risk GWP "
        f"({pct_at_risk:.1f}% of portfolio)"
    )

    display_cols = [
        "quote_id", "policyholder_name", "section", "broker_primary",
        "underwriter", "new_renewal", "expiring_inception_date",
        "expiring_gnwp", "gross_rarc", "net_rarc",
        "claims_inflation", "breadth_of_cover_change",
    ]

    st.dataframe(
        df_flagged[display_cols],
        column_config={
            "quote_id":               "Quote ID",
            "policyholder_name":      "Policyholder",
            "section":                "Section",
            "broker_primary":         "Broker",
            "underwriter":            "Underwriter",
            "new_renewal":            "N/R",
            "expiring_inception_date": st.column_config.DateColumn("Inception"),
            "expiring_gnwp":          st.column_config.NumberColumn(
                                          "Expiring GWP", format="$%,.0f"),
            "gross_rarc":             st.column_config.NumberColumn(
                                          "Gross RARC", format="%.1f%%"),
            "net_rarc":               st.column_config.NumberColumn(
                                          "Net RARC", format="%.1f%%"),
            "claims_inflation":       st.column_config.NumberColumn(
                                          "Claims inflation", format="%.1f%%"),
            "breadth_of_cover_change": st.column_config.NumberColumn(
                                          "Cover change", format="%.1f%%"),
        },
        use_container_width=True,
        hide_index=True,
        height=500,
    )

    foot_l, foot_r = st.columns(2)
    foot_l.markdown(f"**Total at-risk GWP:** ${at_risk_gwp:,.0f}")
    foot_r.markdown(f"**As % of portfolio:** {pct_at_risk:.1f}%")


# ============================================================
# TAB 3 — RARC decomposition
# ============================================================
with tab3:
    dcol_l, dcol_r = st.columns(2)

    # ── Scatter: RARC vs Claims inflation ────────────────────────────────
    with dcol_l:
        if n > 0:
            df_sc = df.copy()
            df_sc["gross_rarc_pct"]     = df_sc["gross_rarc"] * 100
            df_sc["claims_inf_pct"]     = df_sc["claims_inflation"] * 100
            # Size: normalise expiring_gnwp to 4–20
            gnwp_min = df_sc["expiring_gnwp"].min()
            gnwp_max = df_sc["expiring_gnwp"].max()
            gnwp_range = gnwp_max - gnwp_min if gnwp_max > gnwp_min else 1
            df_sc["size_norm"] = 4 + (df_sc["expiring_gnwp"] - gnwp_min) / gnwp_range * 16

            fig_sc1 = px.scatter(
                df_sc,
                x="claims_inf_pct",
                y="gross_rarc_pct",
                color="section",
                size="size_norm",
                size_max=20,
                hover_data={
                    "quote_id": True,
                    "policyholder_name": True,
                    "section": True,
                    "gross_rarc_pct": ":.1f",
                    "claims_inf_pct": ":.1f",
                    "size_norm": False,
                },
                title="Rate adequacy vs claims inflation",
                labels={
                    "claims_inf_pct": "Claims inflation (%)",
                    "gross_rarc_pct": "Gross RARC (%)",
                },
                height=400,
            )
            # Diagonal y=x
            axis_min = min(df_sc["claims_inf_pct"].min(), df_sc["gross_rarc_pct"].min()) - 1
            axis_max = max(df_sc["claims_inf_pct"].max(), df_sc["gross_rarc_pct"].max()) + 1
            fig_sc1.add_shape(
                type="line", x0=axis_min, y0=axis_min, x1=axis_max, y1=axis_max,
                line=dict(color="#888", dash="dot", width=1),
            )
            fig_sc1.add_annotation(
                x=axis_max * 0.85, y=axis_max * 0.85,
                text="RARC = inflation",
                showarrow=False,
                font=dict(size=10, color="#888"),
            )
            fig_sc1.add_hline(y=0, line_dash="dash", line_color="white", line_width=1)
            fig_sc1.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
            )
            st.plotly_chart(fig_sc1, use_container_width=True)

    # ── Scatter: RARC vs Breadth of cover change ─────────────────────────
    with dcol_r:
        if n > 0:
            df_sc2 = df.copy()
            df_sc2["gross_rarc_pct"] = df_sc2["gross_rarc"] * 100
            df_sc2["breadth_pct"]    = df_sc2["breadth_of_cover_change"] * 100
            gnwp_min2 = df_sc2["expiring_gnwp"].min()
            gnwp_max2 = df_sc2["expiring_gnwp"].max()
            gnwp_range2 = gnwp_max2 - gnwp_min2 if gnwp_max2 > gnwp_min2 else 1
            df_sc2["size_norm"] = 4 + (df_sc2["expiring_gnwp"] - gnwp_min2) / gnwp_range2 * 16

            fig_sc2 = px.scatter(
                df_sc2,
                x="breadth_pct",
                y="gross_rarc_pct",
                color="section",
                size="size_norm",
                size_max=20,
                hover_data={
                    "quote_id": True,
                    "policyholder_name": True,
                    "section": True,
                    "gross_rarc_pct": ":.1f",
                    "breadth_pct": ":.1f",
                    "size_norm": False,
                },
                title="Rate movement vs cover structure change",
                labels={
                    "breadth_pct": "Breadth of cover change (%)",
                    "gross_rarc_pct": "Gross RARC (%)",
                },
                height=400,
            )
            fig_sc2.add_hline(y=0, line_dash="dash", line_color="white", line_width=1)
            fig_sc2.add_vline(x=0, line_dash="dash", line_color="white", line_width=1)

            # Quadrant annotations
            x_max = df_sc2["breadth_pct"].max() * 0.9
            x_min = df_sc2["breadth_pct"].min() * 0.9
            y_max = df_sc2["gross_rarc_pct"].max() * 0.85
            y_min = df_sc2["gross_rarc_pct"].min() * 0.85

            for txt, ax, ay in [
                ("Rate up, cover narrowed",   x_min, y_max),
                ("Rate up, cover widened",    x_max, y_max),
                ("Rate down, cover narrowed", x_min, y_min),
                ("Rate down, cover widened",  x_max, y_min),
            ]:
                fig_sc2.add_annotation(
                    x=ax, y=ay, text=txt, showarrow=False,
                    font=dict(size=9, color="#999"),
                    xanchor="center",
                )
            fig_sc2.update_layout(
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(showgrid=False),
                yaxis=dict(showgrid=False),
            )
            st.plotly_chart(fig_sc2, use_container_width=True)

# ---------------------------------------------------------------------------
# Footer
# ---------------------------------------------------------------------------
st.markdown("---")
st.caption(
    "Data product: pricing_adequacy_monitoring_dashboard  ·  "
    "Fact table: rate_monitoring_total_our_share_usd  ·  "
    "Dimension: ll_quote_setup  ·  Join: quote_id  ·  "
    "Grain: one row per quote  ·  300 synthetic records  ·  "
    "Spec generated by enterprise-data-product-planner"
)
