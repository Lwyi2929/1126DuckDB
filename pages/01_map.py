import solara
import pandas as pd
import leafmap.foliumap as leafmap

# ----------------------------------------------------
# 1. 讀取城市資料（全世界城市）
# ----------------------------------------------------
CITIES_CSV_URL = "https://raw.githubusercontent.com/solara-dev/examples/main/public/cities.csv"

# 載入全部資料
all_data = pd.read_csv(CITIES_CSV_URL)
all_countries = sorted(all_data["country"].unique())

# ----------------------------------------------------
# 2. 反應式 state
# ----------------------------------------------------
selected_country = solara.reactive("Taiwan")
filtered_df = solara.reactive(pd.DataFrame())


# ----------------------------------------------------
# 3. 載入指定國家的城市資料
# ----------------------------------------------------
def load_filtered_data(country):
    df = all_data[all_data["country"] == country]
    filtered_df.set(df)
    return df


# ----------------------------------------------------
# 4. 地圖元件
# ----------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):
    m = leafmap.Map(center=[20, 0], zoom=2)

    if len(df) > 0:
        m.add_points_from_xy(
            df,
            x="lng",
            y="lat",
            popup=["city", "lat", "lng"],
            layer_name="Cities",
        )

        # 自動縮放顯示所有城市
        min_lat, max_lat = df["lat"].min(), df["lat"].max()
        min_lon, max_lon = df["lng"].min(), df["lng"].max()
        m.zoom_to_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    return m


# ----------------------------------------------------
# 5. 主頁面 UI
# ----------------------------------------------------
@solara.component
def Page():
    # 載入初始資料
    df = load_filtered_data(selected_country.value)

    with solara.Column(gap="20px"):
        solara.Markdown("## 🌍 國家城市地圖（City Map Viewer）")

        # Country Selector
        solara.Select(
            label="選擇國家",
            value=selected_country.value,
            values=all_countries,
            on_value=lambda v: selected_country.set(v),
        )

        # 當選單變動 → 更新資料
        df = load_filtered_data(selected_country.value)

        # Map
        CityMap(df)

        solara.Markdown("### 📄 城市經緯度表格")
        solara.DataFrame(df[["city", "lat", "lng"]])


# Solara app 入口
Page()
