from __future__ import annotations

"""
Plotly-based chart generation.

Charts are saved as standalone HTML files and auto-opened in the browser.
"""

import webbrowser
from datetime import datetime
from pathlib import Path

import plotly.graph_objects as go
import plotly.express as px

import config


def _save_and_open(fig: go.Figure, title: str) -> str:
    """Save figure as HTML, open in browser, return file path."""
    slug = title.lower().replace(" ", "_")[:40]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{slug}_{ts}.html"
    path = config.CHARTS_DIR / filename
    fig.write_html(str(path), include_plotlyjs="cdn")
    webbrowser.open(f"file://{path.resolve()}")
    return str(path)


def bar_chart(
    data: list[dict],
    x_col: str,
    y_col: str,
    title: str,
    color_col: str | None = None,
    horizontal: bool = False,
) -> str:
    """Simple bar chart. Returns path to saved HTML."""
    import pandas as pd
    df = pd.DataFrame(data)

    if horizontal:
        fig = px.bar(df, x=y_col, y=x_col, color=color_col, title=title, orientation="h")
    else:
        fig = px.bar(df, x=x_col, y=y_col, color=color_col, title=title)

    fig.update_layout(
        plot_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#eee"),
        yaxis=dict(showgrid=True, gridcolor="#eee"),
    )
    return _save_and_open(fig, title)


def stacked_bar_chart(
    data: list[dict],
    x_col: str,
    y_col: str,
    group_col: str,
    title: str,
    barmode: str = "stack",  # "stack" or "group"
) -> str:
    """Stacked/grouped bar chart. Returns path to saved HTML."""
    import pandas as pd
    df = pd.DataFrame(data)
    fig = px.bar(df, x=x_col, y=y_col, color=group_col, title=title, barmode=barmode)
    fig.update_layout(
        plot_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#eee"),
        yaxis=dict(showgrid=True, gridcolor="#eee"),
        legend=dict(title=group_col),
    )
    return _save_and_open(fig, title)


def line_chart(
    data: list[dict],
    x_col: str,
    y_col: str,
    title: str,
    color_col: str | None = None,
) -> str:
    """Line chart for time-series. Returns path to saved HTML."""
    import pandas as pd
    df = pd.DataFrame(data)
    fig = px.line(df, x=x_col, y=y_col, color=color_col, title=title, markers=True)
    fig.update_layout(
        plot_bgcolor="white",
        xaxis=dict(showgrid=True, gridcolor="#eee"),
        yaxis=dict(showgrid=True, gridcolor="#eee"),
    )
    return _save_and_open(fig, title)


def pie_chart(data: list[dict], names_col: str, values_col: str, title: str) -> str:
    """Pie/donut chart. Returns path to saved HTML."""
    import pandas as pd
    df = pd.DataFrame(data)
    fig = px.pie(df, names=names_col, values=values_col, title=title, hole=0.3)
    return _save_and_open(fig, title)


def table_chart(data: list[dict], title: str) -> str:
    """Render data as an interactive sortable table. Returns path to saved HTML."""
    import pandas as pd
    df = pd.DataFrame(data)
    fig = go.Figure(data=[go.Table(
        header=dict(
            values=list(df.columns),
            fill_color="#1a73e8",
            font=dict(color="white", size=12),
            align="left",
        ),
        cells=dict(
            values=[df[col].tolist() for col in df.columns],
            fill_color=[["#f8f9fa", "white"] * (len(df) // 2 + 1)],
            align="left",
            font=dict(size=11),
        )
    )])
    fig.update_layout(title=title)
    return _save_and_open(fig, title)


# ---------------------------------------------------------------------------
# High-level chart builders called by the agent tools
# ---------------------------------------------------------------------------

def chart_spend_by_campaign_group_weekly(data: list[dict]) -> str:
    """Stacked bar: weekly spend per campaign group."""
    return stacked_bar_chart(
        data=data,
        x_col="week",
        y_col="spend_usd",
        group_col="campaign_group_name",
        title="Weekly Spend by Campaign Group",
        barmode="stack",
    )


def chart_demographic_breakdown(data: list[dict], pivot_type: str, campaign_name: str) -> str:
    """Horizontal bar: spend or impressions by demographic facet."""
    label_map = {
        "MEMBER_JOB_TITLE": "Job Title",
        "MEMBER_SENIORITY": "Seniority",
        "MEMBER_INDUSTRY": "Industry",
        "MEMBER_COMPANY": "Company",
        "MEMBER_COMPANY_SIZE": "Company Size",
        "MEMBER_GEOGRAPHY": "Geography",
        "MEMBER_FUNCTION": "Function",
    }
    label = label_map.get(pivot_type, pivot_type)
    # Sort by spend descending, top 25
    import pandas as pd
    df = pd.DataFrame(data).sort_values("spend_usd", ascending=False).head(25)
    return bar_chart(
        data=df.to_dict("records"),
        x_col="pivot_value",
        y_col="spend_usd",
        title=f"{label} Demographics — {campaign_name}",
        horizontal=True,
    )
