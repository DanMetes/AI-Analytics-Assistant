"""Style utilities for consistent UI presentation."""
import streamlit as st

COLORS = {
    "primary": "#1E88E5",
    "secondary": "#6C757D",
    "success": "#28A745",
    "warning": "#FFC107",
    "critical": "#DC3545",
    "info": "#17A2B8",
    "light": "#F8F9FA",
    "dark": "#343A40",
}

SEVERITY_COLORS = {
    "critical": COLORS["critical"],
    "warning": COLORS["warning"],
    "info": COLORS["info"],
}

SEVERITY_ICONS = {
    "critical": "🔴",
    "warning": "🟠",
    "info": "🔵",
}


def format_currency(value: float) -> str:
    """Format a number as currency with thousands separators."""
    if value >= 1_000_000:
        return f"${value / 1_000_000:,.1f}M"
    elif value >= 1_000:
        return f"${value:,.0f}"
    else:
        return f"${value:,.2f}"


def format_number(value: float, decimals: int = 0) -> str:
    """Format a number with thousands separators."""
    if decimals == 0:
        return f"{value:,.0f}"
    return f"{value:,.{decimals}f}"


def format_percent(value: float, decimals: int = 1) -> str:
    """Format a number as percentage."""
    return f"{value * 100:,.{decimals}f}%"


def styled_metric(label: str, value: str, description: str = "", tooltip: str = ""):
    """Render a styled metric with optional description and tooltip."""
    tooltip_html = f' title="{tooltip}"' if tooltip else ""
    st.markdown(f"""
<div style="padding: 12px; background: {COLORS['light']}; border-radius: 8px; margin-bottom: 8px;"{tooltip_html}>
    <div style="font-size: 0.85em; color: {COLORS['secondary']}; margin-bottom: 4px;">{label}</div>
    <div style="font-size: 1.5em; font-weight: 600; color: {COLORS['dark']};">{value}</div>
    {"<div style='font-size: 0.75em; color: " + COLORS['secondary'] + "; margin-top: 4px;'>" + description + "</div>" if description else ""}
</div>
    """, unsafe_allow_html=True)


def severity_badge(severity: str, text: str = "") -> str:
    """Return HTML for a severity badge."""
    color = SEVERITY_COLORS.get(severity, COLORS["secondary"])
    icon = SEVERITY_ICONS.get(severity, "⚪")
    label = text or severity.title()
    return f'<span style="background: {color}; color: white; padding: 2px 8px; border-radius: 12px; font-size: 0.8em;">{icon} {label}</span>'


def section_header(title: str, subtitle: str = ""):
    """Render a styled section header."""
    st.markdown(f"""
<div style="margin: 24px 0 16px 0;">
    <h3 style="margin: 0; color: {COLORS['dark']};">{title}</h3>
    {"<p style='margin: 4px 0 0 0; color: " + COLORS['secondary'] + "; font-size: 0.9em;'>" + subtitle + "</p>" if subtitle else ""}
</div>
    """, unsafe_allow_html=True)


def info_tooltip(text: str, tooltip: str) -> str:
    """Return text with an info icon tooltip."""
    return f'{text} <span title="{tooltip}" style="cursor: help; color: {COLORS["info"]};">ℹ️</span>'


METRIC_GLOSSARY = {
    "units": "The quantity of items sold or processed",
    "sales": "Revenue generated from transactions (in dollars)",
    "profit": "Net earnings after costs are subtracted from sales (in dollars)",
    "profit_margin": "Ratio of profit to sales, expressed as a percentage",
    "discount": "Price reduction applied to items, typically as a percentage",
    "row_count": "Total number of records in the dataset",
    "column_count": "Total number of data fields/columns in the dataset",
    "missing_cells": "Number of empty or null values in the dataset",
    "anomaly": "A data point that deviates significantly from expected patterns",
}
