"""
StravaSquad — analytics dashboard (outside Power BI).

Run with:
"D:\\BO\\New folder\\python.exe" -m streamlit run stravasquad2026\\dashboard\\app.py
"""
import datetime as dt
import os
import sys

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dashboard_lib import (
    ATHLETE_COLORS,
    METHOD_LABELS,
    WEEK_REL_EXPR,
    format_time,
    get_athletes,
    inject_css,
    kpi_card,
    kpi_row,
    load_table,
    metric_card,
    metric_card_html,
    pct_badge,
)

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from predict_pb import (
    riegel,
    compute_vdot,
    vdot_time,
    elevation_adjusted_riegel,
)

st.set_page_config(page_title="StravaSquad", layout="wide", page_icon="🏃")
inject_css()


# ============================================================
# Tab 1 — Dashboard (Weekly squad / Individual)
# ============================================================

def weekly_squad_view():
    athletes = get_athletes()
    n_athletes = len(athletes)

    # --- This/last week ---
    week_df = load_table(
        "WITH w AS ("
        "  SELECT a.activity_id, a.distance_km, a.duration_sec, "
        f"        {WEEK_REL_EXPR} AS week_rel "
        "  FROM silver.Activities a JOIN gold.dim_date d ON a.date = d.[date]"
        ") "
        "SELECT week_rel, SUM(distance_km) AS total_km, "
        "       COUNT(DISTINCT activity_id) AS n_runs, SUM(duration_sec) AS total_sec "
        "FROM w WHERE week_rel IN (0,1) GROUP BY week_rel"
    ).set_index("week_rel")

    this_wk = week_df.loc[0] if 0 in week_df.index else pd.Series({"total_km": 0, "n_runs": 0, "total_sec": 0})
    last_wk = week_df.loc[1] if 1 in week_df.index else pd.Series({"total_km": 0, "n_runs": 0, "total_sec": 0})

    def wow(curr, prev):
        if not prev:
            return None
        return (curr - prev) / prev * 100

    today_df = load_table(
        "SELECT COUNT(DISTINCT athlete_id) AS n "
        "FROM silver.Activities WHERE [date] = CAST(GETDATE() AS date)"
    )
    n_today = int(today_df["n"].iloc[0]) if not today_df.empty else 0

    latest = load_table(
        "SELECT TOP 1 a.athlete_id, ath.firstname, ath.lastname, a.start_time_utc, a.distance_km "
        "FROM silver.Activities a "
        "LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id "
        "ORDER BY a.start_time_utc DESC"
    )

    # --- This month ---
    month_df = load_table(
        "SELECT SUM(a.distance_km) AS km, COUNT(DISTINCT a.activity_id) AS n_runs, SUM(a.duration_sec) AS sec "
        "FROM silver.Activities a "
        "WHERE YEAR(a.[date]) = YEAR(GETDATE()) AND MONTH(a.[date]) = MONTH(GETDATE())"
    )
    m = month_df.iloc[0] if not month_df.empty else pd.Series({"km": 0, "n_runs": 0, "sec": 0})

    # --- Top / bottom runner this week ---
    per_athlete = load_table(
        "WITH w AS ("
        "  SELECT a.athlete_id, ath.firstname, ath.lastname, a.activity_id, a.distance_km, a.duration_sec, "
        f"        {WEEK_REL_EXPR} AS week_rel "
        "  FROM silver.Activities a JOIN gold.dim_date d ON a.date = d.[date] "
        "  LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id"
        ") "
        "SELECT athlete_id, firstname, lastname, "
        "       SUM(distance_km) AS km, COUNT(DISTINCT activity_id) AS n_runs, SUM(duration_sec) AS sec "
        "FROM w WHERE week_rel = 0 GROUP BY athlete_id, firstname, lastname"
    )
    if not per_athlete.empty:
        per_athlete["name"] = (per_athlete["firstname"].fillna("") + " " + per_athlete["lastname"].fillna("")).str.strip()
        per_athlete["name"] = per_athlete["name"].mask(per_athlete["name"] == "", per_athlete["athlete_id"].astype(str))

    def top_bottom(metric, fmt):
        if per_athlete.empty:
            return None, None
        top_row = per_athlete.loc[per_athlete[metric].idxmax()]
        nonzero = per_athlete[per_athlete[metric] > 0]
        bottom_row = nonzero.loc[nonzero[metric].idxmin()] if not nonzero.empty else top_row
        return (
            f"<b>Top Runner</b> {top_row['name']} ({fmt(top_row[metric])})",
            f"<b>Bottom Runner</b> {bottom_row['name']} ({fmt(bottom_row[metric])})",
        )

    # --- PBI-style KPI cards (CSS grid → guaranteed equal height) ---
    st.subheader("This Week at a Glance")
    if not latest.empty:
        row = latest.iloc[0]
        lname = f"{row['firstname'] or ''} {row['lastname'] or ''}".strip() or str(row["athlete_id"])
        latest_html = f"Latest Run: {lname} — {row['start_time_utc']} ({row['distance_km']:.1f} km)"
    else:
        latest_html = "No runs logged yet"

    card1 = (
        f'<div class="metric-card">'
        f'<div class="kpi-label">Athletes Training Today</div>'
        f'<div class="kpi-value">{n_today} / {n_athletes}</div>'
        f'<div class="highlight-strip">{latest_html}</div>'
        f'</div>'
    )

    def make_card(label, value, wk_col, pa_col, fmt, month_val):
        top_line, bottom_line = top_bottom(pa_col, fmt)
        lines = [f"<b>Last Week</b> {fmt(last_wk[wk_col])} {pct_badge(wow(this_wk[wk_col], last_wk[wk_col]))}",
                 f"<b>This Month</b> {month_val}"]
        if top_line:
            lines += [top_line, bottom_line]
        return metric_card_html(label, value, lines)

    kpi_row(
        card1,
        make_card("Total Distance this week", f"{this_wk['total_km']:.2f}",
                  "total_km", "km", lambda v: f"{v:.2f}", f"{(m['km'] or 0):.2f}"),
        make_card("Total Run this week", f"{int(this_wk['n_runs'])}",
                  "n_runs", "n_runs", lambda v: f"{int(v)}", f"{int(m['n_runs'] or 0)}"),
        make_card("Total Runtime this Week", format_time(this_wk["total_sec"]),
                  "total_sec", "sec", format_time, format_time(m["sec"])),
    )

    # --- Daily runs today ---
    st.subheader("Today's Runs")
    today_runs = load_table(
        "SELECT a.athlete_id, ath.firstname, ath.lastname, a.activity_id, "
        "       a.distance_km, a.duration_sec "
        "FROM silver.Activities a "
        "LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id "
        "WHERE a.[date] = CAST(GETDATE() AS date)"
    )
    if today_runs.empty:
        st.info("No runs logged today yet.")
    else:
        today_runs["name"] = (today_runs["firstname"].fillna("") + " " + today_runs["lastname"].fillna("")).str.strip()
        today_runs["name"] = today_runs["name"].mask(today_runs["name"] == "", today_runs["athlete_id"].astype(str))
        today_runs = today_runs[today_runs["distance_km"] > 0].copy()
        today_runs["pace_label"] = (today_runs["duration_sec"] / today_runs["distance_km"]).apply(format_time) + "/km | " + today_runs["distance_km"].apply(lambda v: f"{v:.1f} km")
        fig = px.bar(
            today_runs, y="name", x="distance_km", text="pace_label", orientation="h",
            labels={"name": "Athlete", "distance_km": "Distance (km)"},
            template="plotly_white",
        )
        fig.update_traces(marker_color="#EEF5FD", marker_line_color="#5B5BF7", marker_line_width=1.2,
                          textposition="outside", textfont=dict(color="#111827"))
        fig.update_layout(showlegend=False, xaxis_title="Distance (km)", yaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    # --- This week's GPS tracks ---
    st.subheader("This Week's Run Map")
    track = load_table(
        "WITH this_week AS ("
        "  SELECT a.activity_id, a.athlete_id, ath.firstname, ath.lastname "
        "  FROM silver.Activities a "
        "  JOIN gold.dim_date d ON a.date = d.[date] "
        "  LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id "
        f" WHERE {WEEK_REL_EXPR} = 0"
        ") "
        "SELECT w.athlete_id, w.firstname, w.lastname, rs.lat, rs.lon "
        "FROM this_week w "
        "JOIN silver.RunStream rs ON rs.activity_id = w.activity_id "
        "WHERE rs.lat IS NOT NULL AND rs.lon IS NOT NULL "
        "ORDER BY w.athlete_id, rs.time_s"
    )
    if track.empty:
        st.info("No GPS data for this week yet.")
    else:
        track["name"] = (track["firstname"].fillna("") + " " + track["lastname"].fillna("")).str.strip()
        track["name"] = track["name"].mask(track["name"] == "", track["athlete_id"].astype(str))
        color_map = {n: ATHLETE_COLORS[i % len(ATHLETE_COLORS)] for i, n in enumerate(track["name"].unique())}
        track["color"] = track["name"].map(color_map).apply(lambda h: [int(h[i:i+2], 16) for i in (1, 3, 5)] + [180])
        st.map(track, latitude="lat", longitude="lon", color="color", size=8)

    # --- Squad weekly distance ---
    st.subheader("Squad Weekly Running Distance")
    weekly = load_table(
        "WITH w AS ("
        "  SELECT a.athlete_id, ath.firstname, ath.lastname, a.distance_km, "
        f"        {WEEK_REL_EXPR} AS week_rel, d.week_start_date "
        "  FROM silver.Activities a "
        "  JOIN gold.dim_date d ON a.date = d.[date] "
        "  LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id"
        ") "
        "SELECT week_rel, week_start_date, athlete_id, firstname, lastname, SUM(distance_km) AS km "
        "FROM w WHERE week_rel BETWEEN 0 AND 3 "
        "GROUP BY week_rel, week_start_date, athlete_id, firstname, lastname "
        "ORDER BY week_start_date"
    )
    if not weekly.empty:
        weekly["name"] = (weekly["firstname"].fillna("") + " " + weekly["lastname"].fillna("")).str.strip()
        weekly["name"] = weekly["name"].mask(weekly["name"] == "", weekly["athlete_id"].astype(str))
        weekly["week_label"] = weekly["week_rel"].apply(
            lambda n: "This Week" if n == 0 else "Last Week" if n == 1 else f"{n} Weeks Ago"
        ) + " (" + weekly["week_start_date"].astype(str).str[:10] + ")"
        week_order = weekly.sort_values("week_rel", ascending=False)["week_label"].unique().tolist()
        fig = px.bar(
            weekly, x="week_label", y="km", color="name", barmode="stack",
            labels={"week_label": "Week", "km": "Distance (km)", "name": "Athlete"},
            color_discrete_sequence=ATHLETE_COLORS, template="plotly_white",
            category_orders={"week_label": week_order[::-1]},
        )
        fig.update_layout(xaxis_title="")
        st.plotly_chart(fig, use_container_width=True)

    # --- Scatter: distance vs duration for a selected week ---
    st.subheader("Weekly Volume vs. Time")
    weeks = load_table(
        "WITH w AS ("
        "  SELECT DISTINCT week_start_date, "
        f"        {WEEK_REL_EXPR} AS week_rel "
        "  FROM gold.dim_date d"
        ") "
        "SELECT week_rel, week_start_date FROM w WHERE week_rel >= 0 ORDER BY week_rel"
    )
    if not weeks.empty:
        week_labels = {
            int(r["week_rel"]): (
                "This Week" if r["week_rel"] == 0
                else "Last Week" if r["week_rel"] == 1
                else f"{int(r['week_rel'])} Weeks Ago"
            ) + f" ({r['week_start_date']})"
            for _, r in weeks.iterrows()
        }
        sel = st.selectbox("Week", options=list(week_labels.keys()), format_func=lambda k: week_labels[k])
        scatter_df = load_table(
            "WITH w AS ("
            "  SELECT a.athlete_id, ath.firstname, ath.lastname, a.distance_km, a.duration_sec, "
            f"        {WEEK_REL_EXPR} AS week_rel "
            "  FROM silver.Activities a JOIN gold.dim_date d ON a.date = d.[date] "
            "  LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id"
            ") "
            "SELECT athlete_id, firstname, lastname, SUM(distance_km) AS km, SUM(duration_sec) AS sec "
            f"FROM w WHERE week_rel = {sel} GROUP BY athlete_id, firstname, lastname"
        )
        if scatter_df.empty:
            st.info("No activities in the selected week.")
        else:
            scatter_df["name"] = (scatter_df["firstname"].fillna("") + " " + scatter_df["lastname"].fillna("")).str.strip()
            scatter_df["name"] = scatter_df["name"].mask(scatter_df["name"] == "", scatter_df["athlete_id"].astype(str))
            fig = px.scatter(
                scatter_df, x="km", y="sec", color="name", size="km",
                labels={"km": "Distance (km)", "sec": "Duration (sec)", "name": "Athlete"},
                color_discrete_sequence=ATHLETE_COLORS, template="plotly_white",
            )
            st.plotly_chart(fig, use_container_width=True)

    # --- This week stats per runner ---
    st.subheader("This Week — Runner Stats")
    stats = load_table(
        "WITH w AS ("
        "  SELECT a.athlete_id, ath.firstname, ath.lastname, a.activity_id, "
        "         a.[date], a.distance_km, a.duration_sec, a.cad_avg, a.hr_avg, "
        f"        {WEEK_REL_EXPR} AS week_rel "
        "  FROM silver.Activities a "
        "  JOIN gold.dim_date d ON a.date = d.[date] "
        "  LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id"
        ") "
        "SELECT firstname, lastname, "
        "       COUNT(DISTINCT [date])       AS run_days, "
        "       AVG(cad_avg)                 AS avg_cadence, "
        "       AVG(hr_avg)                  AS avg_hr, "
        "       SUM(distance_km)             AS total_km, "
        "       COUNT(DISTINCT activity_id)  AS run_count, "
        "       SUM(duration_sec)            AS total_sec "
        "FROM w WHERE week_rel = 0 "
        "GROUP BY athlete_id, firstname, lastname "
        "ORDER BY total_km DESC"
    )
    if not stats.empty:
        stats["name"] = (stats["firstname"].fillna("") + " " + stats["lastname"].fillna("")).str.strip()
        stats["Total Time"] = stats["total_sec"].apply(format_time)
        display = stats[[
            "name", "run_days", "avg_cadence", "avg_hr", "total_km", "run_count", "Total Time",
        ]].rename(columns={
            "name": "Name",
            "run_days": "Run Days",
            "avg_cadence": "Avg Cadence",
            "avg_hr": "Avg HR",
            "total_km": "Distance (km)",
            "run_count": "Runs",
        })
        st.dataframe(display.style.format({
            "Avg Cadence": "{:.1f}", "Avg HR": "{:.1f}", "Distance (km)": "{:.1f}",
        }, na_rep="-"), use_container_width=True)
    else:
        st.info("No activities recorded this week yet.")


def individual_view():
    athletes = get_athletes()
    name = st.selectbox("Athlete", athletes["name"].tolist())
    athlete_id = int(athletes.loc[athletes["name"] == name, "athlete_id"].iloc[0])

    week_df = load_table(
        "WITH w AS ("
        "  SELECT a.activity_id, a.distance_km, a.duration_sec, "
        f"        {WEEK_REL_EXPR} AS week_rel "
        "  FROM silver.Activities a JOIN gold.dim_date d ON a.date = d.[date] "
        f"  WHERE a.athlete_id = {athlete_id}"
        ") "
        "SELECT week_rel, SUM(distance_km) AS total_km, "
        "       COUNT(DISTINCT activity_id) AS n_runs, SUM(duration_sec) AS total_sec "
        "FROM w WHERE week_rel IN (0,1) GROUP BY week_rel"
    ).set_index("week_rel")

    this_wk = week_df.loc[0] if 0 in week_df.index else pd.Series({"total_km": 0, "n_runs": 0, "total_sec": 0})
    last_wk = week_df.loc[1] if 1 in week_df.index else pd.Series({"total_km": 0, "n_runs": 0, "total_sec": 0})

    def wow(curr, prev):
        if not prev:
            return None
        return (curr - prev) / prev * 100

    # Athlete detail card + 3 KPI cards in one equal-height row
    detail = load_table(
        "SELECT sex, weight, city, country FROM silver.Athlete "
        f"WHERE athlete_id = {athlete_id}"
    )
    d = detail.iloc[0] if not detail.empty else pd.Series({"sex": None, "weight": None, "city": None, "country": None})
    rows_html = "".join(
        f'<div class="detail-row"><b>{lbl}</b> {val}</div>'
        for lbl, val in [
            ("Sex", d["sex"] or "-"),
            ("Weight", f"{d['weight']:.0f} kg" if pd.notna(d["weight"]) else "-"),
            ("City", d["city"] or "-"),
            ("Country", d["country"] or "-"),
        ]
    )
    detail_html = (
        f'<div class="metric-card">'
        f'<div class="kpi-label">Athlete</div>'
        f'<div class="kpi-value" style="font-size:1.3rem">{name}</div>'
        f'<div class="kpi-divider"></div>'
        f'{rows_html}'
        f'</div>'
    )
    kpi_row(
        detail_html,
        metric_card_html(
            "This week (km)", f"{this_wk['total_km']:.2f}",
            [f"<b>Last Week</b> {last_wk['total_km']:.2f} {pct_badge(wow(this_wk['total_km'], last_wk['total_km']))}"],
        ),
        metric_card_html(
            "This Week", f"{int(this_wk['n_runs'])}",
            [f"<b>Last Week</b> {int(last_wk['n_runs'])} {pct_badge(wow(this_wk['n_runs'], last_wk['n_runs']))}"],
        ),
        metric_card_html(
            "This Week (hh:mm)", format_time(this_wk["total_sec"]),
            [f"<b>Last Week</b> {format_time(last_wk['total_sec'])} {pct_badge(wow(this_wk['total_sec'], last_wk['total_sec']))}"],
        ),
    )

    # --- Activity history chart (Strava-style) ---
    c_metric, c_grain = st.columns([3, 2])
    with c_metric:
        metric_sel = st.radio("Metric", ["Distance", "Time", "Elev Gain"], horizontal=True, label_visibility="collapsed")
    with c_grain:
        grain_sel = st.radio("Grain", ["Weekly", "Monthly"], horizontal=True, label_visibility="collapsed", index=1)

    if grain_sel == "Monthly":
        hist = load_table(
            "SELECT DATEADD(month, DATEDIFF(month,0,[date]),0) AS period, "
            "       SUM(distance_km) AS distance_km, SUM(duration_sec) AS duration_sec, "
            "       SUM(elev_gain_m) AS elev_gain_m "
            f"FROM silver.Activities WHERE athlete_id = {athlete_id} "
            "  AND [date] >= DATEADD(month, -12, GETDATE()) "
            "GROUP BY DATEADD(month, DATEDIFF(month,0,[date]),0) ORDER BY period"
        )
    else:
        hist = load_table(
            "SELECT d.week_start_date AS period, "
            "       SUM(a.distance_km) AS distance_km, SUM(a.duration_sec) AS duration_sec, "
            "       SUM(a.elev_gain_m) AS elev_gain_m "
            f"FROM silver.Activities a JOIN gold.dim_date d ON a.date = d.[date] "
            f"WHERE a.athlete_id = {athlete_id} "
            "  AND a.[date] >= DATEADD(week, -52, GETDATE()) "
            "GROUP BY d.week_start_date ORDER BY period"
        )
    if not hist.empty:
        if metric_sel == "Distance":
            y_col, y_title = "distance_km", "Distance (km)"
        elif metric_sel == "Time":
            hist["hours"] = (hist["duration_sec"] / 3600).round(2)
            y_col, y_title = "hours", "Time (h)"
        else:
            y_col, y_title = "elev_gain_m", "Elevation Gain (m)"
        fig = px.bar(hist, x="period", y=y_col, labels={"period": "", y_col: y_title},
                     template="plotly_white", color_discrete_sequence=["#33A2D9"])
        fig.update_layout(showlegend=False, bargap=0.25)
        st.plotly_chart(fig, use_container_width=True)

    # --- Bubble chart: last 4 weeks x day-of-week ---
    st.subheader("Last 4 Weeks")
    bubble = load_table(
        "WITH w AS ("
        "  SELECT d.[date], d.day_name, d.day_of_week, "
        f"        {WEEK_REL_EXPR} AS week_rel "
        "  FROM gold.dim_date d"
        ") "
        "SELECT w.week_rel, w.day_name, w.day_of_week, w.[date], "
        "       COALESCE(SUM(a.distance_km), 0) AS km "
        "FROM w "
        f"LEFT JOIN silver.Activities a ON a.date = w.[date] AND a.athlete_id = {athlete_id} "
        "WHERE w.week_rel BETWEEN 0 AND 3 "
        "GROUP BY w.week_rel, w.day_name, w.day_of_week, w.[date] "
        "ORDER BY w.week_rel, w.day_of_week"
    )
    if not bubble.empty:
        def week_label(n):
            return "This Week" if n == 0 else "Last Week" if n == 1 else f"{n} Weeks Ago"

        bubble["week_label"] = bubble["week_rel"].apply(week_label)
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        week_order = [week_label(n) for n in sorted(bubble["week_rel"].unique(), reverse=True)]
        plot_df = bubble[bubble["km"] > 0].copy()
        plot_df["km_label"] = plot_df["km"].apply(lambda v: f"{v:.1f}")
        fig = px.scatter(
            plot_df, x="day_name", y="week_label", size="km", color_discrete_sequence=[ATHLETE_COLORS[0]],
            category_orders={"day_name": day_order, "week_label": week_order},
            labels={"day_name": "Day", "week_label": "Week", "km": "Distance (km)"},
            hover_data={"km": ":.1f"}, text="km_label", template="plotly_white",
        )
        fig.update_traces(
            marker=dict(sizemode="area", sizeref=2. * bubble["km"].max() / (40. ** 2) if bubble["km"].max() else 1),
            textposition="middle center", textfont=dict(color="white", size=10),
        )
        fig.update_xaxes(categoryorder="array", categoryarray=day_order)
        fig.update_yaxes(categoryorder="array", categoryarray=week_order)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No activity data available.")

    # --- Shared activity selector ---
    recent = load_table(
        "SELECT TOP 20 activity_id, [date], start_time_utc, distance_km, duration_sec "
        f"FROM silver.Activities WHERE athlete_id = {athlete_id} "
        "ORDER BY start_time_utc DESC"
    )
    if recent.empty:
        st.info("No activities recorded for this athlete.")
        return

    recent["label"] = recent.apply(
        lambda r: f"{r['date']} — {r['distance_km']:.1f} km ({format_time(r['duration_sec'])})", axis=1
    )
    sel_label = st.selectbox("Activity", recent["label"].tolist(), key="shared_act_sel")
    sel_activity_id = int(recent.loc[recent["label"] == sel_label, "activity_id"].iloc[0])
    sel_row = recent.loc[recent["label"] == sel_label].iloc[0]

    # --- Personal Bests + run map (shared selector) ---
    col_pb, col_map = st.columns(2)
    with col_pb:
        st.subheader("Personal Bests")
        pb = load_table(
            "SELECT dl.sort_order, dl.distance_label, MIN(rb.elapsed_time) AS pb_sec "
            "FROM gold.dim_distance_label dl "
            f"LEFT JOIN silver.RunBest rb ON rb.name = dl.distance_label AND rb.athlete_id = {athlete_id} "
            "GROUP BY dl.sort_order, dl.distance_label ORDER BY dl.sort_order"
        )
        if not pb.empty:
            pb["Personal Best"] = pb["pb_sec"].apply(format_time)
            st.dataframe(
                pb[["distance_label", "Personal Best"]].rename(columns={"distance_label": "Distance"}),
                use_container_width=True, hide_index=True,
            )
    with col_map:
        st.subheader("Run Map")
        track = load_table(
            "SELECT lat, lon FROM silver.RunStream "
            f"WHERE activity_id = {sel_activity_id} AND lat IS NOT NULL AND lon IS NOT NULL "
            "ORDER BY time_s"
        )
        if track.empty:
            st.info("No GPS data for this run.")
        else:
            track["color"] = [[91, 91, 247, 200]] * len(track)
            st.map(track, latitude="lat", longitude="lon", color="color", size=6)

    # --- Overview KPIs + km splits + HR/Watts (shared selector) ---
    kpi_row(
        metric_card_html("Distance", f"{sel_row['distance_km']:.2f} km"),
        metric_card_html("Total Time", format_time(sel_row["duration_sec"])),
    )

    splits = load_table(
        "SELECT segment_number, pace_sec_per_km "
        f"FROM silver.RunSplitKilometer WHERE activity_id = {sel_activity_id} "
        "ORDER BY segment_number"
    )
    if not splits.empty:
        splits["Average Pace"] = splits["pace_sec_per_km"].apply(format_time) + "/km"
        st.dataframe(
            splits[["segment_number", "Average Pace"]].rename(columns={"segment_number": "Kilometer"}),
            use_container_width=True, hide_index=True,
        )

    stream = load_table(
        "SELECT time_s, hr_bpm, watts FROM silver.RunStream "
        f"WHERE activity_id = {sel_activity_id} ORDER BY time_s"
    )
    if not stream.empty and (stream["hr_bpm"].notna().any() or stream["watts"].notna().any()):
        fig = go.Figure()
        if stream["hr_bpm"].notna().any():
            fig.add_trace(go.Scatter(x=stream["time_s"], y=stream["hr_bpm"],
                                     name="Heart Rate (bpm)", line=dict(color=ATHLETE_COLORS[0])))
        if stream["watts"].notna().any():
            fig.add_trace(go.Scatter(x=stream["time_s"], y=stream["watts"],
                                     name="Watts", line=dict(color=ATHLETE_COLORS[1])))
        fig.update_layout(template="plotly_white", title="Heart Rate and Watts by Time", xaxis_title="Time (s)")
        st.plotly_chart(fig, use_container_width=True)


def dashboard_tab():
    view = st.radio("View", ["Weekly (Squad)", "Individual"], horizontal=True, label_visibility="collapsed")
    if view == "Weekly (Squad)":
        weekly_squad_view()
    else:
        individual_view()


# ============================================================
# Tab 2 — Prediction (race forecasts + PB trends + readiness)
# ============================================================

def race_forecast_page(target_label, title, race_date):
    df = load_table(
        f"SELECT rf.*, a.firstname, a.lastname "
        f"FROM gold.RaceForecast rf "
        f"LEFT JOIN silver.Athlete a ON a.athlete_id = rf.athlete_id "
        f"WHERE rf.target_distance_label = '{target_label}' "
        f"ORDER BY rf.athlete_id, rf.method, rf.as_of_date"
    )
    if df.empty:
        st.warning(f"No RaceForecast rows for {target_label}. Run predict_pb.py first.")
        return

    df["athlete_name"] = (df["firstname"].fillna("") + " " + df["lastname"].fillna("")).str.strip()
    df["athlete_name"] = df["athlete_name"].mask(df["athlete_name"] == "", df["athlete_id"].astype(str))
    df["method_label"] = df["method"].map(METHOD_LABELS)

    st.header(title)
    st.caption(f"Race date: {race_date} — predictions computed using only data available before each as-of date.")

    # Final prediction (closest as-of date) per athlete/method
    final = df.loc[df.groupby(["athlete_id", "method"])["as_of_date"].idxmax()].copy()
    final["predicted_fmt"] = final["predicted_sec"].apply(format_time)
    final["actual_fmt"] = final["actual_sec"].apply(format_time)
    final["error"] = final["error_pct"].round(1)

    st.subheader("Final predictions vs actual result")
    pivot = final.pivot(index="athlete_name", columns="method_label", values="error")
    actuals = final.groupby("athlete_name")["actual_fmt"].first()
    pivot.insert(0, "Actual time", actuals)
    st.dataframe(
        pivot.style.format(precision=1, na_rep="-")
        .background_gradient(cmap="RdYlGn_r", subset=[c for c in pivot.columns if c != "Actual time"], vmin=-30, vmax=30),
        use_container_width=True,
    )
    st.caption("Cell values = signed error %, predicted vs actual (negative = predicted faster than actual).")

    st.subheader("Which algorithm got closest? (mean absolute % error across athletes)")
    summary = final.groupby("method_label")["error_pct"].apply(lambda s: s.abs().mean()).reset_index()
    summary.columns = ["Method", "Mean abs. % error"]
    summary = summary.sort_values("Mean abs. % error")
    fig = px.bar(summary, x="Method", y="Mean abs. % error", text_auto=".1f",
                  color_discrete_sequence=[ATHLETE_COLORS[0]], template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Trace-back: how predictions evolved as race day approached")
    athlete_options = final["athlete_name"].unique().tolist()
    selected = st.selectbox("Athlete", athlete_options, key=f"traceback_{target_label}")
    sub = df[df["athlete_name"] == selected].sort_values("as_of_date")

    fig2 = go.Figure()
    for method, grp in sub.groupby("method_label"):
        fig2.add_trace(go.Scatter(
            x=grp["as_of_date"], y=grp["predicted_sec"] / 60.0,
            mode="lines+markers", name=method,
        ))
    actual_sec = sub["actual_sec"].iloc[0]
    fig2.add_hline(y=actual_sec / 60.0, line_dash="dash", line_color="black",
                    annotation_text=f"Actual: {format_time(actual_sec)}")
    fig2.update_layout(xaxis_title="As-of date", yaxis_title="Predicted time (minutes)",
                        title=f"{selected} — {target_label} forecast trace-back",
                        template="plotly_white")
    st.plotly_chart(fig2, use_container_width=True)

    with st.expander("Raw forecast rows"):
        st.dataframe(df.drop(columns=["firstname", "lastname"]), use_container_width=True)


def pb_trend_page():
    st.header("Personal Best Trends")

    athletes = load_table(
        "SELECT DISTINCT a.athlete_id, ath.firstname, ath.lastname "
        "FROM silver.RunBest a "
        "LEFT JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id "
        "ORDER BY a.athlete_id"
    )
    athletes["name"] = (athletes["firstname"].fillna("") + " " + athletes["lastname"].fillna("")).str.strip()
    athletes["name"] = athletes["name"].mask(athletes["name"] == "", athletes["athlete_id"].astype(str))

    col1, col2 = st.columns(2)
    with col1:
        athlete_name = st.selectbox("Athlete", athletes["name"].tolist(), key="pb_trend_athlete")
    athlete_id = int(athletes.loc[athletes["name"] == athlete_name, "athlete_id"].iloc[0])

    distances = load_table(
        f"SELECT DISTINCT name FROM silver.RunBest WHERE athlete_id = {athlete_id} ORDER BY name"
    )["name"].tolist()
    with col2:
        distance_label = st.selectbox("Distance", distances, key="pb_trend_distance")

    history = load_table(
        f"SELECT start_date, elapsed_time FROM silver.RunBest "
        f"WHERE athlete_id = {athlete_id} AND name = '{distance_label}' "
        f"ORDER BY start_date"
    )
    # Exclude the known clamped-to-1-second placeholder rows (meaningless extrapolation).
    prediction = load_table(
        f"SELECT predicted_best_sec, prediction_date, r_squared FROM gold.PBPrediction "
        f"WHERE athlete_id = {athlete_id} AND distance_label = '{distance_label}' "
        f"AND predicted_best_sec > 1"
    )

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=history["start_date"], y=history["elapsed_time"] / 60.0,
        mode="markers+lines", name="PB history",
    ))
    if not prediction.empty:
        pred_row = prediction.iloc[0]
        fig.add_trace(go.Scatter(
            x=[history["start_date"].iloc[-1], pred_row["prediction_date"]],
            y=[history["elapsed_time"].iloc[-1] / 60.0, pred_row["predicted_best_sec"] / 60.0],
            mode="lines+markers", name="Trend projection", line=dict(dash="dot"),
        ))
        st.caption(
            f"Projected best as of {pred_row['prediction_date']}: "
            f"{format_time(pred_row['predicted_best_sec'])} (R² = {pred_row['r_squared']:.2f})"
        )
    else:
        st.caption("Not enough history (<3 records), or trend projection was not meaningful for this distance.")

    fig.update_layout(xaxis_title="Date", yaxis_title="Time (minutes)",
                       title=f"{athlete_name} — {distance_label} PB trend",
                       template="plotly_white")
    st.plotly_chart(fig, use_container_width=True)


def readiness_page():
    RACE_DATE = dt.date(2025, 10, 26)
    RACE_LABEL = "Half-Marathon 2025-10-26"
    TRAIN_WEEKS = 16
    train_start = RACE_DATE - dt.timedelta(weeks=TRAIN_WEEKS)
    taper_start = RACE_DATE - dt.timedelta(weeks=3)

    st.header(f"Training Readiness — {RACE_LABEL}")
    st.caption(
        f"16-week training block ({train_start} → {RACE_DATE}). "
        "Taper zone = last 3 weeks. Long run target ≥ 18 km."
    )

    # Only show athletes who actually ran the HM
    hm_athletes = load_table(
        "SELECT DISTINCT a.athlete_id, ath.firstname, ath.lastname "
        "FROM silver.Activities a "
        "JOIN silver.Athlete ath ON ath.athlete_id = a.athlete_id "
        f"WHERE a.distance_km BETWEEN 19 AND 23 "
        f"AND a.[date] BETWEEN '2025-10-24' AND '2025-10-28'"
    )
    all_athletes = get_athletes()
    if not hm_athletes.empty:
        hm_athletes["name"] = (hm_athletes["firstname"].fillna("") + " " + hm_athletes["lastname"].fillna("")).str.strip()
        hm_ids = set(hm_athletes["athlete_id"].astype(str))
        options = all_athletes[all_athletes["athlete_id"].astype(str).isin(hm_ids)]["name"].tolist()
        if not options:
            options = all_athletes["name"].tolist()
    else:
        options = all_athletes["name"].tolist()

    name = st.selectbox("Athlete", options, key="readiness_athlete")
    athlete_id = int(all_athletes.loc[all_athletes["name"] == name, "athlete_id"].iloc[0])

    weekly = load_table(
        "SELECT d.week_start_date, "
        "       SUM(a.distance_km)            AS km, "
        "       MAX(a.distance_km)            AS long_run_km, "
        "       COUNT(DISTINCT a.activity_id) AS n_runs, "
        "       AVG(a.hr_avg)                 AS avg_hr "
        "FROM silver.Activities a JOIN gold.dim_date d ON a.date = d.[date] "
        f"WHERE a.athlete_id = {athlete_id} "
        f"  AND a.[date] >= '{train_start}' AND a.[date] <= '{RACE_DATE}' "
        "GROUP BY d.week_start_date ORDER BY d.week_start_date"
    )

    if weekly.empty:
        st.info("No training data found for this athlete in the 16-week window.")
        return

    weekly["ramp_pct"] = weekly["km"].pct_change() * 100
    weekly["week_start_date"] = pd.to_datetime(weekly["week_start_date"])
    race_dt = pd.Timestamp(RACE_DATE)
    taper_dt = pd.Timestamp(taper_start)

    peak_km      = weekly["km"].max()
    peak_week    = weekly.loc[weekly["km"].idxmax(), "week_start_date"]
    longest_run  = weekly["long_run_km"].max()
    consistency  = (weekly["n_runs"] >= 3).mean() * 100
    taper_mask   = weekly["week_start_date"] >= taper_dt
    taper_km     = weekly.loc[taper_mask, "km"].iloc[0] if taper_mask.any() else None
    taper_drop   = ((taper_km - peak_km) / peak_km * 100) if taper_km and peak_km else None

    kpi_row(
        metric_card_html("Peak Week Volume", f"{peak_km:.1f} km",
                         [f"<b>Week of</b> {peak_week.strftime('%b %d')}"]),
        metric_card_html("Longest Run", f"{longest_run:.1f} km",
                         [f"{'✓ Hit 18 km target' if longest_run >= 18 else '✗ Below 18 km target'}"]),
        metric_card_html("Consistency", f"{consistency:.0f}%",
                         ["% of weeks with 3+ runs"]),
        metric_card_html("Taper Week Volume", f"{taper_km:.1f} km" if taper_km else "-",
                         [f"<b>Drop from peak:</b> {taper_drop:+.0f}%" if taper_drop else ""]),
    )

    race_str  = RACE_DATE.isoformat()
    taper_str = taper_start.isoformat()

    def add_race_markers(fig):
        existing_shapes = list(fig.layout.shapes or [])
        existing_annots = list(fig.layout.annotations or [])
        fig.update_layout(
            shapes=existing_shapes + [
                dict(type="rect", xref="x", yref="paper",
                     x0=taper_str, x1=race_str, y0=0, y1=1,
                     fillcolor="#FEF3C7", opacity=0.3, line_width=0),
                dict(type="line", xref="x", yref="paper",
                     x0=race_str, x1=race_str, y0=0, y1=1,
                     line=dict(color="#DC2626", dash="dash", width=2)),
            ],
            annotations=existing_annots + [
                dict(xref="x", yref="paper", x=race_str, y=1.02,
                     text="Race Day", showarrow=False, font=dict(color="#DC2626", size=11)),
                dict(xref="x", yref="paper", x=taper_str, y=1.02,
                     text="Taper", showarrow=False, font=dict(color="#92400E", size=11)),
            ],
        )
        return fig

    st.subheader("Weekly Distance")
    fig = px.bar(weekly, x="week_start_date", y="km",
                 labels={"week_start_date": "Week", "km": "Distance (km)"},
                 color_discrete_sequence=[ATHLETE_COLORS[0]], template="plotly_white")
    fig = add_race_markers(fig)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Long Run Progression")
    fig = px.line(weekly, x="week_start_date", y="long_run_km", markers=True,
                  labels={"week_start_date": "Week", "long_run_km": "Longest Run (km)"},
                  color_discrete_sequence=[ATHLETE_COLORS[1]], template="plotly_white")
    fig.add_hline(y=18, line_dash="dot", line_color="#16A34A",
                  annotation_text="18 km target", annotation_position="top left")
    fig = add_race_markers(fig)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Ramp Rate")
    ramp_df = weekly.dropna(subset=["ramp_pct"])
    ramp_df = ramp_df.copy()
    ramp_df["color"] = ramp_df["ramp_pct"].apply(
        lambda v: "#DC2626" if abs(v) > 30 else "#F59E0B" if abs(v) > 10 else "#16A34A"
    )
    fig = go.Figure(go.Bar(
        x=ramp_df["week_start_date"], y=ramp_df["ramp_pct"],
        marker_color=ramp_df["color"],
    ))
    fig.update_layout(template="plotly_white", xaxis_title="Week", yaxis_title="WoW change (%)")
    fig.add_hrect(y0=-10, y1=10, fillcolor="#BBF7D0", opacity=0.25, line_width=0,
                  annotation_text="Safe zone ±10%", annotation_position="top left")
    fig = add_race_markers(fig)
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Weekly Run Count")
    fig = px.bar(weekly, x="week_start_date", y="n_runs",
                 labels={"week_start_date": "Week", "n_runs": "Runs"},
                 color_discrete_sequence=[ATHLETE_COLORS[2]], template="plotly_white")
    fig.add_hline(y=3, line_dash="dot", line_color="#6B7280",
                  annotation_text="3 runs/wk minimum", annotation_position="top left")
    fig = add_race_markers(fig)
    st.plotly_chart(fig, use_container_width=True)


def prediction_tab():
    page = st.radio(
        "Prediction view",
        ["Half-Marathon Backtest", "Marathon Backtest", "PB Trends", "Training Readiness"],
        horizontal=True, label_visibility="collapsed",
    )
    if page == "Half-Marathon Backtest":
        race_forecast_page("Half-Marathon", "Half-Marathon Forecast Backtest — 6 athletes", dt.date(2025, 10, 26))
    elif page == "Marathon Backtest":
        race_forecast_page("Marathon", "Marathon Forecast Backtest — Tam_Vu (bonus case)", dt.date(2025, 10, 5))
    elif page == "PB Trends":
        pb_trend_page()
    else:
        readiness_page()


# ============================================================
# Tab 3 — What-If Race Simulator
# ============================================================

SOURCE_PRIORITY = ["10K", "15K", "5K", "20K", "10 mile", "1 mile", "30K"]


@st.cache_data(ttl=300)
def _load_distance_labels():
    df = load_table(
        "SELECT distance_label, standard_distance_m, sort_order "
        "FROM gold.dim_distance_label ORDER BY sort_order"
    )
    return df


@st.cache_data(ttl=30)
def _load_athlete_pbs(athlete_id):
    df = load_table(f"""
        SELECT rb.name, MIN(rb.elapsed_time) AS pb_sec
        FROM silver.RunBest rb
        WHERE rb.athlete_id = {int(athlete_id)}
          AND rb.name IN ('400m','1/2 mile','1K','1 mile','2 mile',
                          '5K','10K','15K','10 mile','20K',
                          'Half-Marathon','30K','Marathon')
        GROUP BY rb.name
    """)
    return dict(zip(df["name"], df["pb_sec"]))


def whatif_simulator_tab():
    st.header("What-If Race Simulator")
    st.caption("Adjust your source PB and race conditions to explore predicted finish times.")

    dist_df = _load_distance_labels()
    dist_map = dict(zip(dist_df["distance_label"], dist_df["standard_distance_m"]))
    all_labels = dist_df["distance_label"].tolist()

    athletes = get_athletes()

    col_input, col_output = st.columns([1, 2])

    # ── Input panel ──
    with col_input:
        sel_name = st.selectbox("Athlete", athletes["name"], key="wif_athlete")
        sel_athlete_id = int(athletes.loc[athletes["name"] == sel_name, "athlete_id"].iloc[0])

        pbs = _load_athlete_pbs(sel_athlete_id)

        st.markdown("---")
        st.subheader("Source PB")

        available_sources = [d for d in SOURCE_PRIORITY if d in pbs]
        if not available_sources:
            st.warning("No RunBest records found for this athlete.")
            return

        source_label = st.selectbox("Source distance", available_sources, key="wif_src")
        actual_pb_sec = float(pbs[source_label])
        st.markdown(f"**Actual PB:** {format_time(actual_pb_sec)}")

        delta_sec = st.slider(
            "Adjust source PB (seconds)",
            min_value=-300, max_value=300, value=0, step=10,
            help="Negative = faster, Positive = slower",
            key="wif_delta",
        )
        adjusted_pb_sec = actual_pb_sec + delta_sec
        if delta_sec != 0:
            st.markdown(f"**Adjusted PB:** {format_time(adjusted_pb_sec)}")

        st.markdown("---")
        st.subheader("Target Race")

        target_default = all_labels.index("Half-Marathon") if "Half-Marathon" in all_labels else 0
        target_label = st.selectbox("Target distance", all_labels, index=target_default, key="wif_tgt")
        target_distance_m = dist_map[target_label]

        target_elev = st.slider(
            "Course elevation (m/km)", min_value=0.0, max_value=50.0,
            value=5.0, step=0.5, key="wif_elev",
        )

        with st.expander("Advanced"):
            riegel_exp = st.slider(
                "Riegel exponent", min_value=1.00, max_value=1.15,
                value=1.06, step=0.01, key="wif_exp",
            )

    # ── Compute predictions ──
    source_distance_m = dist_map[source_label]

    pred_riegel = riegel(adjusted_pb_sec, source_distance_m, target_distance_m, riegel_exp)

    try:
        vdot_val = compute_vdot(source_distance_m, adjusted_pb_sec)
        pred_vdot = vdot_time(vdot_val, target_distance_m)
    except Exception:
        vdot_val = None
        pred_vdot = None

    pred_elev = elevation_adjusted_riegel(
        adjusted_pb_sec, source_distance_m, 0.0,
        target_distance_m, target_elev,
    )

    target_km = target_distance_m / 1000.0

    # ── Output panel ──
    with col_output:
        # KPI cards
        vdot_card = metric_card_html(
            "Your VDOT",
            f"{vdot_val:.1f}" if vdot_val else "N/A",
            ["Higher = fitter runner"],
        )
        riegel_card = metric_card_html(
            "Riegel",
            format_time(pred_riegel),
            [f"Pace: {format_time(pred_riegel / target_km)}/km"],
        )
        vdot_time_card = metric_card_html(
            "VDOT (Daniels)",
            format_time(pred_vdot) if pred_vdot else "N/A",
            [f"Pace: {format_time(pred_vdot / target_km)}/km"] if pred_vdot else [],
        )
        elev_card = metric_card_html(
            "Elevation-adjusted",
            format_time(pred_elev),
            [
                f"Pace: {format_time(pred_elev / target_km)}/km",
                f"Elevation: {target_elev:.1f} m/km",
            ],
        )
        kpi_row(vdot_card, riegel_card, vdot_time_card, elev_card)

        # ── Bar chart: method comparison ──
        methods_data = [
            {"Method": "Riegel", "Predicted (min)": pred_riegel / 60},
            {"Method": "Elevation-adj", "Predicted (min)": pred_elev / 60},
        ]
        if pred_vdot:
            methods_data.insert(1, {"Method": "VDOT (Daniels)", "Predicted (min)": pred_vdot / 60})

        bar_df = pd.DataFrame(methods_data)
        fig_bar = px.bar(
            bar_df, x="Method", y="Predicted (min)",
            color="Method",
            color_discrete_sequence=["#5B5BF7", "#06B6D4", "#F59E0B"],
            text=bar_df["Predicted (min)"].apply(lambda m: format_time(m * 60)),
        )

        # Reference line: actual PB at target distance if one exists
        target_pb = pbs.get(target_label)
        if target_pb:
            fig_bar.add_hline(
                y=float(target_pb) / 60, line_dash="dash", line_color="#DC2626",
                annotation_text=f"Actual PB: {format_time(float(target_pb))}",
                annotation_position="top left",
            )

        fig_bar.update_layout(
            template="plotly_white", showlegend=False,
            yaxis_title="Time (minutes)", height=320,
            margin=dict(t=30, b=30),
        )
        fig_bar.update_traces(textposition="outside")
        st.plotly_chart(fig_bar, use_container_width=True)

        # ── Sensitivity curve ──
        st.subheader("Sensitivity: How Source PB Affects Prediction")
        sweep = np.linspace(actual_pb_sec * 0.7, actual_pb_sec * 1.3, 50)
        curves = []
        for s in sweep:
            r = riegel(s, source_distance_m, target_distance_m, riegel_exp)
            curves.append({"Source PB (min)": s / 60, "Predicted (min)": r / 60, "Method": "Riegel"})
            e = elevation_adjusted_riegel(s, source_distance_m, 0.0, target_distance_m, target_elev)
            curves.append({"Source PB (min)": s / 60, "Predicted (min)": e / 60, "Method": "Elev-adjusted"})
            try:
                v = vdot_time(compute_vdot(source_distance_m, s), target_distance_m)
                curves.append({"Source PB (min)": s / 60, "Predicted (min)": v / 60, "Method": "VDOT"})
            except Exception:
                pass

        curve_df = pd.DataFrame(curves)
        fig_sens = px.line(
            curve_df, x="Source PB (min)", y="Predicted (min)", color="Method",
            color_discrete_sequence=["#5B5BF7", "#F59E0B", "#06B6D4"],
        )

        # Highlight current selection
        fig_sens.add_trace(go.Scatter(
            x=[adjusted_pb_sec / 60], y=[pred_riegel / 60],
            mode="markers", marker=dict(size=14, color="#DC2626", symbol="star"),
            name="Your selection",
        ))

        if target_pb:
            fig_sens.add_hline(
                y=float(target_pb) / 60, line_dash="dot", line_color="#9CA3AF",
                annotation_text=f"Actual {target_label} PB",
                annotation_position="top left",
            )

        fig_sens.update_layout(
            template="plotly_white", height=380,
            margin=dict(t=30, b=30),
            xaxis_title=f"Source {source_label} Time (min)",
            yaxis_title=f"Predicted {target_label} Time (min)",
        )
        st.plotly_chart(fig_sens, use_container_width=True)


# ============================================================
# Main
# ============================================================

def main():
    st.title("StravaSquad")
    st.caption("Squad analytics, race-time predictions, and what-if race simulator.")

    tab1, tab2, tab3 = st.tabs(["Dashboard", "Prediction", "What-If Simulator"])
    with tab1:
        dashboard_tab()
    with tab2:
        prediction_tab()
    with tab3:
        whatif_simulator_tab()


if __name__ == "__main__":
    main()
