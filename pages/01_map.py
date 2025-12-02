import solara
import pandas as pd
import leafmap

# 你的城市資料（可換為你的 CSV Source）
CITIES_CSV_URL = "https://raw.githubusercontent.com/datasets/world-cities/master/data/world-cities.csv"

# -----------------------------------------------------------
# Load all cities
# -----------------------------------------------------------
@solara.memo()
def load_all_cities():
    df = pd.read_csv(CITIES_CSV_URL)
    df.rename(columns={"name": "city"}, inplace=True)
    df = df[["country", "city", "lat", "lng"]]  # 統一欄位名稱
    return df

all_cities_df = load_all_cities()

# 取得全部國家名稱
all_countries = sorted(all_cities_df["country"].unique())

selected_country = solara.reactive("")
table_df = solara.reactive(pd.DataFrame())


# -----------------------------------------------------------
# Filter the cities of selected country
# -----------------------------------------------------------
def load_country_cities(country_name):
    if not country_name:
        return pd.DataFrame()

    df = all_cities_df[all_cities_df["country"] == country_name].copy()
    df.reset_index(drop=True, inplace=True)
    return df


# -----------------------------------------------------------
# Map Component
# -----------------------------------------------------------
@solara.component
def CityMap(country, df: pd.DataFrame):

    # Leafmap + Solara 用 style 控制大小（不能加 width="100%"）
    m = leafmap.Map(
        style={"width": "100%", "height": "600px"},  
        center=[20, 0],
        zoom=2
    )

    # 清除城市點
    if not df.empty:
        for i, row in df.iterrows():
            m.add_marker(
                location=[row.lat, row.lng],
                popup=f"{row.city}, {row.country}",
            )

        # 自動縮放至所有城市
        min_lat = df["lat"].min()
        max_lat = df["lat"].max()
        min_lon = df["lng"].min()
        max_lon = df["lng"].max()
        m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    return m


# -----------------------------------------------------------
# Page UI
# -----------------------------------------------------------
@solara.component
def Page():

    def on_country_select(value):
        selected_country.set(value)
        df = load_country_cities(value)
        table_df.set(df)

    return solara.Column(
        [
            solara.Markdown("## 🌍 選擇國家並顯示城市座標"),

            solara.Select(
                label="選擇國家",
                value=selected_country.value,
                values=all_countries,
                on_value=on_country_select,
            ),

            solara.Div(style="height: 20px"),  # spacing

            # 地圖
            CityMap(selected_country.value, table_df.value),

            solara.Div(style="height: 20px"),

            solara.Markdown("### 📍 城市列表（含經緯度）"),

            solara.DataTable(
                table_df.value,
                columns=["city", "lat", "lng"],
                items_per_page=10,
            ),
        ]
    )