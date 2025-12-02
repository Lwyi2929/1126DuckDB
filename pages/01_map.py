import solara
import pandas as pd
import leafmap

# -----------------------------------------------------------
# Module-level cache (舊版 Solara 最相容的寫法)
# -----------------------------------------------------------
CITIES_CSV_URL = "https://raw.githubusercontent.com/datasets/world-cities/master/data/world-cities.csv"

all_cities_df = pd.read_csv(CITIES_CSV_URL)
all_cities_df.rename(columns={"name": "city"}, inplace=True)
all_cities_df = all_cities_df[["country", "city", "lat", "lng"]]

all_countries = sorted(all_cities_df["country"].unique())

# reactive 狀態
selected_country = solara.reactive("")
filtered_df = solara.reactive(pd.DataFrame())


# -----------------------------------------------------------
# Filter function
# -----------------------------------------------------------
def load_country_cities(country):
    if not country:
        return pd.DataFrame()

    df = all_cities_df[all_cities_df["country"] == country].copy()
    df.reset_index(drop=True, inplace=True)
    return df


# -----------------------------------------------------------
# Map Component（不使用 use_memo）
# -----------------------------------------------------------

@solara.component
def CityMap(df: pd.DataFrame):

    m = leafmap.Map(
        style={"width": "100%", "height": "600px"},
        center=[20, 0],
        zoom=2
    )

    if not df.empty:
        for _, row in df.iterrows():
            m.add_marker(
                location=[row.lat, row.lng],
                popup=f"{row.city}, {row.country}",
            )

        # fit bounds
        min_lat = df["lat"].min()
        max_lat = df["lat"].max()
        min_lon = df["lng"].min()
        max_lon = df["lng"].max()

        m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    return m


# -----------------------------------------------------------
# Page UI（完全相容舊版 Solara）
# -----------------------------------------------------------

@solara.component
def Page():

    def on_country_change(value):
        selected_country.set(value)
        filtered_df.set(load_country_cities(value))

    return solara.Column(
        [
            solara.Markdown("## 🌍 選擇國家並顯示城市座標"),

            solara.Select(
                label="國家",
                value=selected_country.value,
                values=all_countries,
                on_value=on_country_change,
            ),

            solara.Div(style="height: 20px"),

            CityMap(filtered_df.value),

            solara.Div(style="height: 20px"),

            solara.Markdown("### 📍 城市列表"),

            solara.DataTable(
                filtered_df.value,
                columns=["city", "lat", "lng"],
                items_per_page=10
            ),
        ]
    )