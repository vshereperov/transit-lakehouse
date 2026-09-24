from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

STYLE = Path(__file__).parent / "style.css"
SERVING = "https://sttransitlake.blob.core.windows.net/serving/rome"

TITLE = "Rome transit speed"
SUBTITLE = "Median speed of buses and trams on each street, from live GPS"

METRIC = "median_speed_kmh"

MIN_SEGMENTS = 20

WEEK_H = 7 * 24

DOW_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
]

PALETTE = [
    [252, 206, 37], [252, 166, 54], [242, 132, 75], [225, 100, 98],
    [204, 71, 120], [177, 42, 144], [143, 13, 164],
]

EDGES = np.array([5, 10, 15, 20, 30])

ROME = (41.893, 12.482)
ZOOM = 11
LINE_M = 25
LINE_PX_MIN = 2
LINE_PX_MAX = 8

ATTRIBUTION = (
    '<span class="attribution">'
    '© <a href="https://carto.com/attributions" target="_blank">CARTO</a> '
    '© <a href="https://www.openstreetmap.org/copyright" target="_blank">OpenStreetMap</a>'
    "</span>"
)

TOOLTIP: Any = {
    "html": (
        '<div class="tooltip-street">{street}</div>'
        '<div class="tooltip-speed"><i style="background:{swatch}"></i>'
        '<b>{median_speed_kmh} km/h</b><span class="tooltip-muted">median</span></div>'
        '<div class="tooltip-muted">{vehicles} vehicles · {routes} routes</div>'
        '<div class="tooltip-muted">moving {moving_pct}% of the time</div>'
    ),
    "style": {
        "color": "#fff",
        "backgroundColor": "var(--glass-bg)",
        "backdropFilter": "var(--glass-blur)",
        "WebkitBackdropFilter": "var(--glass-blur)",
        "border": "var(--glass-border)",
        "borderRadius": "12px",
        "boxShadow": "var(--glass-shadow)",
        "padding": "10px 14px",
        "lineHeight": "1.4",
    },
}


@st.cache_data(ttl=3600)
def load() -> pd.DataFrame:
    sas = st.secrets["serving_sas"]
    speed = pd.read_parquet(f"{SERVING}/street_speed.parquet?{sas}")
    speed["dow_name"] = speed["dow"].map(dict(enumerate(DOW_NAMES)))
    geometry = pd.read_parquet(f"{SERVING}/street_geometry.parquet?{sas}")
    geometry["path"] = geometry["path"].apply(
        lambda path: [[float(lon), float(lat)] for lon, lat in path]
    )
    return speed.merge(geometry, on="way_id")


def classify(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    return np.where(values > 0, np.searchsorted(edges, values, side="left") + 1, 0)


def css_rgb(colour: list[int]) -> str:
    r, g, b = colour
    return f"rgb({r},{g},{b})"


def select(df: pd.DataFrame, day: str, hour: int) -> pd.DataFrame:
    view = df[(df["dow_name"] == day) & (df["hour_rome"] == hour)]
    view = view[view["segments"] >= MIN_SEGMENTS]
    colours = [PALETTE[i] for i in classify(view[METRIC].to_numpy(), EDGES)]
    return view.assign(
        colour=colours,
        swatch=[css_rgb(c) for c in colours],
        moving_pct=(view["moving_share"] * 100).round().astype(int),
    ).sort_values(METRIC, ascending=False)


def nearest_slot(df: pd.DataFrame) -> tuple[int, int]:
    now = pd.Timestamp.now(tz="Europe/Rome")
    here = now.weekday() * 24 + now.hour
    measured = df[df["segments"] >= MIN_SEGMENTS]
    slots = np.unique(measured["dow"] * 24 + measured["hour_rome"])
    ahead = (slots - here) % WEEK_H
    behind = WEEK_H - ahead
    distance = np.minimum(ahead, behind)
    best = slots[np.lexsort((ahead < behind, distance))[0]]
    return divmod(int(best), 24)


def controls(df: pd.DataFrame) -> tuple[str, int]:
    if "start" not in st.session_state:
        st.session_state["start"] = nearest_slot(df)
    start_dow, start_hour = st.session_state["start"]

    with st.container(key="controls"):
        st.title(TITLE, anchor=False)
        st.caption(SUBTITLE)
        day = st.segmented_control(
            "Day",
            DOW_NAMES,
            default=DOW_NAMES[start_dow],
            required=True,
            format_func=lambda d: d[:3],
            width="stretch",
        )
        hour = st.slider("Hour", 0, 23, start_hour, format="%02d:00")
    return day, hour


def speed_map(view: pd.DataFrame) -> None:
    st.pydeck_chart(
        pdk.Deck(
            map_style="dark",
            initial_view_state=pdk.ViewState(
                latitude=ROME[0], longitude=ROME[1], zoom=ZOOM
            ),
            layers=[
                pdk.Layer(
                    "PathLayer",
                    data=view,
                    get_path="path",
                    get_color="colour",
                    get_width=LINE_M,
                    width_min_pixels=LINE_PX_MIN,
                    width_max_pixels=LINE_PX_MAX,
                    cap_rounded=True,
                    joint_rounded=True,
                    pickable=True,
                    auto_highlight=True,
                    highlight_color=[255, 255, 255, 220],
                )
            ],
            tooltip=TOOLTIP,
        ),
        width="stretch",
    )


def scale() -> str:
    stopped, *moving = (css_rgb(c) for c in PALETTE)
    swatches = "".join(f'<i style="background:{c}"></i>' for c in moving)
    ticks = "".join(
        f'<span style="left:{100 * i / len(moving):.2f}%">{edge}</span>'
        for i, edge in enumerate(EDGES, start=1)
    )
    return (
        '<div class="speed-legend">'
        f'<div class="stop"><i style="background:{stopped}"></i><span>stop</span></div>'
        f'<div class="scale"><div class="bar">{swatches}</div>'
        f'<div class="ticks">{ticks}<span class="unit">km/h</span></div></div>'
        "</div>"
    )


def legend(view: pd.DataFrame, day: str, hour: int) -> None:
    window = f"{hour:02d}:00–{(hour + 1) % 24:02d}:00"
    with st.container(key="legend"):
        if view.empty:
            st.caption(f"No data for {day}, {window} yet. Try another hour or day.")
            st.markdown(ATTRIBUTION, unsafe_allow_html=True)
            return

        st.markdown(scale(), unsafe_allow_html=True)
        dates = int(view["days"].max())
        span = f"the last {dates} {day}s" if dates > 1 else f"the last {day}"
        st.caption(f"{view['segments'].sum():,} measurements, {window} on {span}")
        with st.container(
            horizontal=True,
            horizontal_alignment="distribute",
            vertical_alignment="center",
            gap="xsmall",
        ):
            table_button(view)
            st.markdown(ATTRIBUTION, unsafe_allow_html=True, width="content")


@st.fragment
def table_button(view: pd.DataFrame) -> None:
    if st.button("Table", icon=":material/table_rows:", type="tertiary"):
        show_table(view)


@st.dialog("Streets", width="large")
def show_table(view: pd.DataFrame) -> None:
    columns = [
        "street", METRIC, "avg_speed_kmh", "moving_share", "routes", "vehicles", "segments",
    ]
    st.dataframe(
        view[columns].sort_values(METRIC),
        column_config={
            "street": st.column_config.TextColumn("Street"),
            METRIC: st.column_config.NumberColumn("Median, km/h", format="%.1f"),
            "avg_speed_kmh": st.column_config.NumberColumn("Average, km/h", format="%.1f"),
            "moving_share": st.column_config.NumberColumn("Moving", format="percent"),
            "routes": st.column_config.NumberColumn("Routes"),
            "vehicles": st.column_config.NumberColumn("Vehicles"),
            "segments": st.column_config.NumberColumn("Measurements"),
        },
        width="stretch",
        hide_index=True,
    )


st.set_page_config(page_title=TITLE, page_icon=":material/directions_bus:", layout="wide")
st.html(f"<style>{STYLE.read_text(encoding='utf-8')}</style>")

df = load()
day, hour = controls(df)
view = select(df, day, hour)
speed_map(view)
legend(view, day, hour)
