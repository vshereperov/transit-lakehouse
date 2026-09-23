from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pydeck as pdk
import streamlit as st

DATA = Path(__file__).parent / "data"
SPEED = DATA / "street_speed.parquet"
GEOMETRY = DATA / "street_geometry.parquet"

METRIC = "median_speed_kmh"

MIN_SEGMENTS = 20

DOW_NAMES = [
    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
]

PALETTE = [
    [255, 40, 40], [255, 130, 30], [255, 230, 60], [150, 240, 60],
    [40, 220, 120], [40, 220, 230], [80, 140, 255],
]

ROME = (41.893, 12.482)
ZOOM = 11
LINE_M = 6
LINE_PX_MIN = 1
LINE_PX_MAX = 4


@st.cache_data
def load() -> pd.DataFrame:
    speed = pd.read_parquet(SPEED)
    speed["dow_name"] = speed["dow"].map(dict(enumerate(DOW_NAMES)))
    geometry = pd.read_parquet(GEOMETRY)
    geometry["path"] = geometry["path"].apply(
        lambda path: [[float(lon), float(lat)] for lon, lat in path]
    )
    return speed.merge(geometry, on="way_id")


def class_edges(values: pd.Series) -> np.ndarray:
    """Quantiles over the streets that moved. A median of zero is a state, not a speed,
    and there are enough of them to swallow a whole class and squash the rest."""
    quantiles = np.linspace(0, 1, len(PALETTE))
    return values[values > 0].quantile(quantiles).to_numpy()[1:-1]


def classify(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    """Class 0 is reserved for stopped, so moving streets start at 1."""
    return np.where(values > 0, np.searchsorted(edges, values, side="left") + 1, 0)


st.set_page_config(page_title="Rome transit speed", layout="wide")
st.title("Rome transit speed")

missing = [f.name for f in (SPEED, GEOMETRY) if not f.exists()]
if missing:
    st.error(f"Missing {', '.join(missing)} in {DATA}. Export them from Databricks.")
    st.stop()

df = load()

EDGES = class_edges(df[df["segments"] >= MIN_SEGMENTS][METRIC])

left, middle = st.columns(2)
with left:
    present = df.groupby("dow_name")["segments"].sum().sort_values(ascending=False)
    days = [d for d in DOW_NAMES if d in present.index]
    day = st.selectbox("Day", days, index=days.index(present.index[0]))
with middle:
    hour = st.slider("Hour", 0, 23, 8)
view = df[(df["dow_name"] == day) & (df["hour_rome"] == hour)]
view = view[view["segments"] >= MIN_SEGMENTS]

if view.empty:
    st.warning("Nothing recorded for this combination yet. Try another hour or day.")
    st.stop()

classes = classify(view[METRIC].to_numpy(), EDGES)
view = view.assign(
    colour=[PALETTE[i] for i in classes],
    moving_pct=(view["moving_share"] * 100).round().astype(int),
)

TOOLTIP: Any = {
    "html": (
        "<b>{street}</b><br/>"
        "<b>{median_speed_kmh} km/h</b> median, {avg_speed_kmh} average<br/>"
        "{segments} observations, {vehicles} vehicles, {routes} routes<br/>"
        "moving {moving_pct}% of the time, over {days} day(s)"
    )
}

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
                opacity=0.9,
                pickable=True,
            )
        ],
        tooltip=TOOLTIP,
    ),
    width="stretch",
)

bar = "".join(
    f'<td style="background:rgb({r},{g},{b});height:14px"></td>' for r, g, b in PALETTE
)
slowest, fastest = view[METRIC].min(), view[METRIC].max()
st.markdown(
    f'<table style="width:100%;border-spacing:0"><tr>{bar}</tr></table>'
    '<div style="display:flex;justify-content:space-between;font-size:0.8rem">'
    f"<span>{slowest:.0f} km/h</span><span>{fastest:.0f} km/h</span></div>",
    unsafe_allow_html=True,
)

dates = int(view["days"].max())
span = f"the last {dates} {day}s" if dates > 1 else f"the last {day}"
st.caption(f"{view['segments'].sum():,} measurements over {span}.")

with st.expander("Table"):
    st.dataframe(
        view[
            [
                "street", "highway", "median_speed_kmh", "avg_speed_kmh",
                "moving_share", "segments", "vehicles", "routes", "days", "snap_m",
            ]
        ].sort_values(METRIC),
        width="stretch",
        hide_index=True,
    )
