"""
StravaSquad dashboard — shared data-access and styling helpers.
"""
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

DB_NAME = "StravaDW"
SERVER = "HANG"
DRIVER = "ODBC Driver 17 for SQL Server"

ATHLETE_COLORS = [
    "#5B5BF7", "#22C55E", "#F59E0B", "#06B6D4", "#EC4899",
    "#8B5CF6", "#F97316", "#10B981", "#64748B",
]

METHOD_LABELS = {
    "riegel": "Riegel",
    "vdot": "VDOT (Daniels)",
    "elevation_adjusted": "Elevation-adjusted Riegel",
}

# gold.dim_date is a materialized dbt table, so its `week_relative_to_today` column
# is frozen at the date dbt last ran - NOT live. Recompute it at query time instead,
# anchored to the current week's Monday (week_start_date values are always exact
# Mondays, so the day-difference is always an exact multiple of 7).
WEEK_REL_EXPR = (
    "DATEDIFF(day, d.week_start_date, "
    "DATEADD(day, (DATEDIFF(day, 0, CAST(GETDATE() AS date)) / 7) * 7, 0)) / 7"
)


@st.cache_resource
def get_engine():
    return create_engine(
        f"mssql+pyodbc://@{SERVER}/{DB_NAME}"
        f"?driver={DRIVER.replace(' ', '+')}"
        "&Trusted_Connection=yes",
        fast_executemany=True,
        future=True,
        connect_args={"timeout": 30},
    )


@st.cache_data(ttl=30)
def load_table(query):
    with get_engine().connect() as conn:
        return pd.read_sql(text(query), conn)


def format_time(seconds):
    if pd.isna(seconds):
        return "-"
    seconds = int(round(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}:{m:02d}:{s:02d}"
    return f"{m}:{s:02d}"


@st.cache_data(ttl=30)
def get_athletes():
    df = load_table(
        "SELECT athlete_id, firstname, lastname FROM silver.Athlete ORDER BY athlete_id"
    )
    df["name"] = (df["firstname"].fillna("") + " " + df["lastname"].fillna("")).str.strip()
    df["name"] = df["name"].mask(df["name"] == "", df["athlete_id"].astype(str))
    return df


def pct_badge(pct):
    """Render a green/red percent-change badge, PBI-style. None -> no badge."""
    if pct is None:
        return ""
    cls = "badge-pos" if pct >= 0 else "badge-neg"
    return f'<span class="{cls}">{pct:+.1f}%</span>'


def metric_card_html(label, value, lines=None):
    """Return HTML for a PBI-style card (big value + optional detail lines)."""
    divider = '<div class="kpi-divider"></div>' if lines else ""
    lines_html = "".join(f'<div class="kpi-sub">{line}</div>' for line in (lines or []))
    return (
        f'<div class="metric-card">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f'{divider}{lines_html}'
        f'</div>'
    )


def metric_card(label, value, lines=None):
    """Render a single PBI-style card via st.markdown."""
    st.markdown(metric_card_html(label, value, lines), unsafe_allow_html=True)


def kpi_row(*cards_html):
    """Render N card HTML strings in a CSS-grid row — guarantees equal height."""
    cols = " ".join(["1fr"] * len(cards_html))
    inner = "".join(cards_html)
    st.markdown(
        f'<div style="display:grid;grid-template-columns:{cols};gap:1rem;align-items:stretch;">'
        f'{inner}</div>',
        unsafe_allow_html=True,
    )


def kpi_card(label, value, sub=None):
    """Render a styled KPI card (for values st.metric can't express well)."""
    sub_html = f'<div class="kpi-sub">{sub}</div>' if sub else ""
    st.markdown(
        f"""
        <div class="metric-card">
            <div class="kpi-label">{label}</div>
            <div class="kpi-value">{value}</div>
            {sub_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


def inject_css():
    st.markdown(
        """
        <style>
        .stApp { background-color: #F9FAFB; }

        [data-testid="stMetric"] {
            background-color: #FFFFFF;
            border: 1px solid #EEF0F3;
            border-radius: 16px;
            padding: 1rem 1.25rem;
            box-shadow: 0 1px 3px rgba(0,0,0,.04), 0 4px 12px rgba(0,0,0,.03);
        }
        [data-testid="stMetricLabel"] {
            color: #6B7280;
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .05em;
        }
        [data-testid="stMetricValue"] {
            color: #111827;
            font-weight: 700;
        }

        .metric-card {
            background-color: #FFFFFF;
            border: 1px solid #EEF0F3;
            border-radius: 16px;
            padding: 1.1rem 1.25rem;
            box-shadow: 0 1px 3px rgba(0,0,0,.04), 0 4px 12px rgba(0,0,0,.03);
            margin-bottom: .75rem;
            height: 100%;
            box-sizing: border-box;
        }
        [data-testid="column"] { display: flex; flex-direction: column; }
        [data-testid="column"] > div:first-child { flex: 1; display: flex; flex-direction: column; }
        [data-testid="column"] > div:first-child > div { flex: 1; }
        .kpi-label {
            color: #6B7280;
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .05em;
            margin-bottom: .35rem;
        }
        .kpi-value {
            color: #111827;
            font-size: 2rem;
            font-weight: 700;
            line-height: 1.1;
        }
        .kpi-sub {
            color: #6B7280;
            font-size: .85rem;
            margin-top: .25rem;
        }

        .highlight-strip {
            background-color: #EAF3FF;
            color: #1E3A8A;
            border-radius: 10px;
            padding: .5rem .75rem;
            margin-top: .75rem;
            font-size: .85rem;
        }

        .kpi-divider {
            border-top: 1px solid #EEF0F3;
            margin: .6rem 0 .5rem;
        }
        .badge-pos, .badge-neg {
            display: inline-block;
            padding: .05rem .4rem;
            border-radius: 6px;
            font-size: .8rem;
            font-weight: 600;
            margin-left: .3rem;
        }
        .badge-pos { background-color: #BBF7D0; color: #166534; }
        .badge-neg { background-color: #FECACA; color: #991B1B; }

        .detail-card {
            background-color: #FFFFFF;
            border: 1px solid #EEF0F3;
            border-radius: 16px;
            padding: 1.1rem 1.25rem;
            box-shadow: 0 1px 3px rgba(0,0,0,.04), 0 4px 12px rgba(0,0,0,.03);
            margin-bottom: .75rem;
        }
        .detail-card .detail-title {
            color: #6B7280;
            font-size: .8rem;
            text-transform: uppercase;
            letter-spacing: .05em;
            margin-bottom: .35rem;
        }
        .detail-card .detail-name {
            color: #111827;
            font-size: 1.4rem;
            font-weight: 700;
            margin-bottom: .6rem;
        }
        .detail-card .detail-row {
            color: #374151;
            font-size: .9rem;
            line-height: 1.6;
        }

        h1, h2, h3 { color: #111827; }
        </style>
        """,
        unsafe_allow_html=True,
    )
