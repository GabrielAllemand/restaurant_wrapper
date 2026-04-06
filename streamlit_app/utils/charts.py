from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def _base_layout(fig: go.Figure, *, height: int = 420) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=10, r=10, t=60, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(size=13),
        legend=dict(
            orientation="v",
            yanchor="top",
            y=1,
            xanchor="left",
            x=1.01,
            bgcolor="rgba(255,255,255,0.7)",
        ),
    )
    return fig


def _normalize_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _infer_map_icon(category: object, subcategory: object) -> str:
    text = f"{_normalize_text(category)} {_normalize_text(subcategory)}"

    if any(word in text for word in ["restaurant", "fast_food", "fast food", "bistro", "brasserie"]):
        return "restaurant"
    if any(word in text for word in ["boulanger", "patisser", "pâtisser", "croissant", "pain"]):
        return "bakery"
    if any(word in text for word in ["bar", "pub", "cafe", "café"]):
        return "bar"
    if any(word in text for word in ["supermarche", "supermarché", "epicer", "épicer", "grocery", "market"]):
        return "grocery"
    if any(word in text for word in ["pharmacie", "pharmacy"]):
        return "pharmacy"
    if any(word in text for word in ["hotel", "hôtel", "lodging"]):
        return "lodging"
    if any(word in text for word in ["coiff", "beaute", "beauté", "hair", "salon"]):
        return "hairdresser"

    return "marker"


def _infer_map_emoji(category: object, subcategory: object) -> str:
    text = f"{_normalize_text(category)} {_normalize_text(subcategory)}"

    if any(word in text for word in ["restaurant", "fast_food", "fast food", "bistro", "brasserie"]):
        return "🍽️"
    if any(word in text for word in ["boulanger", "patisser", "pâtisser", "croissant", "pain"]):
        return "🥐"
    if any(word in text for word in ["bar", "pub", "cafe", "café"]):
        return "☕"
    if any(word in text for word in ["supermarche", "supermarché", "epicer", "épicer", "grocery", "market", "commerce", "superette", "supérette"]):
        return "🛒"
    if any(word in text for word in ["pharmacie", "pharmacy"]):
        return "➕"
    if any(word in text for word in ["hotel", "hôtel", "lodging"]):
        return "🛏️"
    if any(word in text for word in ["coiff", "beaute", "beauté", "hair", "salon"]):
        return "✂️"
    if any(word in text for word in ["boucher", "viande"]):
        return "🥩"
    if any(word in text for word in ["fromager", "fromage"]):
        return "🧀"
    if any(word in text for word in ["glacier"]):
        return "🍦"

    return "•"


def bar_chart(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    title: str,
    color: str | None = None,
    height: int = 420,
) -> go.Figure:
    chart_df = df.copy()

    fig = px.bar(
        chart_df,
        x=x,
        y=y,
        color=color,
        text_auto=True,
        title=title,
    )
    fig.update_traces(marker_line_width=0, opacity=0.92)
    fig.update_xaxes(title=None, tickangle=-35)
    fig.update_yaxes(title=None, gridcolor="rgba(148,163,184,0.22)")
    return _base_layout(fig, height=height)


def donut_chart(
    df: pd.DataFrame,
    *,
    names: str,
    values: str,
    title: str,
    height: int = 420,
) -> go.Figure:
    chart_df = df.copy()

    fig = px.pie(
        chart_df,
        names=names,
        values=values,
        hole=0.58,
        title=title,
    )
    fig.update_traces(
        textposition="inside",
        textinfo="percent+label",
        pull=[0.02] * len(chart_df),
    )
    return _base_layout(fig, height=height)


def heatmap(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    z: str,
    title: str,
    height: int = 520,
) -> go.Figure:
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=title, height=height)
        return _base_layout(fig, height=height)

    pivot = df.pivot(index=y, columns=x, values=z).fillna(0)

    fig = px.imshow(
        pivot,
        text_auto=True,
        aspect="auto",
        title=title,
    )
    fig.update_xaxes(title=None, tickangle=-35)
    fig.update_yaxes(title=None)
    return _base_layout(fig, height=height)


def scatter_priority(
    df: pd.DataFrame,
    *,
    x: str,
    y: str,
    size: str,
    color: str,
    hover_name: str,
    title: str,
    height: int = 460,
) -> go.Figure:
    chart_df = df.copy()

    fig = px.scatter(
        chart_df,
        x=x,
        y=y,
        size=size,
        color=color,
        hover_name=hover_name,
        title=title,
        size_max=42,
    )
    fig.update_traces(opacity=0.88, marker_line_width=0)
    fig.update_xaxes(gridcolor="rgba(148,163,184,0.22)")
    fig.update_yaxes(gridcolor="rgba(148,163,184,0.22)")
    return _base_layout(fig, height=height)

def map_scatter(
    df: pd.DataFrame,
    *,
    color_col: str = "category",
    title: str = "Carte",
    height: int = 650,
) -> go.Figure:
    chart_df = df.copy()

    if chart_df.empty:
        fig = go.Figure()
        fig.update_layout(title=title, height=height)
        return _base_layout(fig, height=height)

    chart_df = chart_df[
        chart_df["latitude"].notna()
        & chart_df["longitude"].notna()
    ].copy()

    if chart_df.empty:
        fig = go.Figure()
        fig.update_layout(title=title, height=height)
        return _base_layout(fig, height=height)

    chart_df["display_name"] = chart_df.get("name", pd.Series(index=chart_df.index)).fillna("Unnamed")
    chart_df["display_category"] = chart_df.get(color_col, pd.Series(index=chart_df.index)).fillna("Non renseigné")
    chart_df["display_subcategory"] = chart_df.get("subcategory", pd.Series(index=chart_df.index)).fillna("Non renseigné")

    city_display = (
        chart_df.get("paris_arrondissement")
        .fillna(chart_df.get("city_canonical"))
        .fillna(chart_df.get("city_business"))
        .fillna(chart_df.get("city"))
    )
    chart_df["display_city"] = city_display.fillna("Zone inconnue")

    chart_df["display_address"] = chart_df.get("address", pd.Series(index=chart_df.index)).fillna("Adresse non renseignée")
    chart_df["display_siret"] = chart_df.get("siret", pd.Series(index=chart_df.index)).fillna("Non renseigné")
    chart_df["display_owner"] = chart_df.get("inpi_rne_representative_name", pd.Series(index=chart_df.index)).fillna("Non renseigné")

    creation_raw = pd.to_datetime(
        chart_df.get("inpi_rne_date_creation", pd.Series(index=chart_df.index)),
        errors="coerce",
    )
    chart_df["display_creation_date"] = creation_raw.dt.strftime("%Y-%m-%d").fillna("Non renseignée")

    chart_df["display_company_name"] = chart_df.get("inpi_rne_company_name", pd.Series(index=chart_df.index)).fillna("Non renseigné")
    chart_df["display_phone"] = chart_df.get("phone", pd.Series(index=chart_df.index)).fillna("Non renseigné")
    chart_df["display_website"] = chart_df.get("website", pd.Series(index=chart_df.index)).fillna("Non renseigné")
    chart_df["map_emoji"] = chart_df.apply(
        lambda row: _infer_map_emoji(row.get(color_col), row.get("subcategory")),
        axis=1,
    )

    center_lat = float(chart_df["latitude"].median())
    center_lon = float(chart_df["longitude"].median())

    lat_span = float(chart_df["latitude"].max() - chart_df["latitude"].min()) if len(chart_df) > 1 else 0.01
    lon_span = float(chart_df["longitude"].max() - chart_df["longitude"].min()) if len(chart_df) > 1 else 0.01
    span = max(lat_span, lon_span)

    if span < 0.03:
        zoom = 13
    elif span < 0.08:
        zoom = 12
    elif span < 0.2:
        zoom = 11
    elif span < 0.5:
        zoom = 10
    elif span < 1.5:
        zoom = 8.7
    elif span < 4:
        zoom = 7.2
    elif span < 8:
        zoom = 6.2
    else:
        zoom = 4.6

    fig = px.scatter_mapbox(
        chart_df,
        lat="latitude",
        lon="longitude",
        color=color_col if color_col in chart_df.columns else None,
        hover_name="display_name",
        hover_data={
            "display_city": True,
            "display_address": True,
            "display_siret": True,
            "display_owner": True,
            "display_creation_date": True,
            "display_company_name": True,
            "display_phone": True,
            "display_website": True,
            "display_subcategory": True,
            "latitude": False,
            "longitude": False,
            "map_emoji": False,
            color_col: False,
        },
        zoom=zoom,
        center={"lat": center_lat, "lon": center_lon},
        height=height,
        title=title,
    )

    fig.update_traces(
        marker=dict(size=9, opacity=0.55),
        hovertemplate=(
            "<b>%{hovertext}</b><br>"
            "Zone : %{customdata[0]}<br>"
            "Adresse : %{customdata[1]}<br>"
            "SIRET : %{customdata[2]}<br>"
            "Propriétaire / représentant : %{customdata[3]}<br>"
            "Date de création : %{customdata[4]}<br>"
            "Nom juridique : %{customdata[5]}<br>"
            "Téléphone : %{customdata[6]}<br>"
            "Site web : %{customdata[7]}<br>"
            "Sous-catégorie : %{customdata[8]}<extra></extra>"
        ),
    )

    fig.add_trace(
        go.Scattermapbox(
            lat=chart_df["latitude"],
            lon=chart_df["longitude"],
            mode="text",
            text=chart_df["map_emoji"],
            textfont=dict(size=12),
            hoverinfo="skip",
            showlegend=False,
        )
    )

    fig.update_layout(
        mapbox_style="carto-positron",
        margin=dict(l=10, r=10, t=60, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
    )

    return _base_layout(fig, height=height)