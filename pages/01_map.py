import solara
import duckdb
import leafmap.maplibregl as leafmap
import pandas as pd

# -----------------------------------------------------------
# 設定
# -----------------------------------------------------------
CITIES_CSV_URL = "https://data.gishub.org/duckdb/cities.csv"

all_countries = solara.reactive([])
selected_country = solara.reactive("TWN")
data_df = solara.reactive(pd.DataFrame())
status_message = solara.reactive("初始化中...")


# -----------------------------------------------------------
# 載入國家列表
# -----------------------------------------------------------
def load_country_list():
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")

        rows = con.sql(f"""
            SELECT DISTINCT country 
            FROM '{CITIES_CSV_URL}'
            ORDER BY country;
        """).fetchall()

        all_countries.set([r[0] for r in rows])
        con.close()

        status_message.set("國家列表載入完成")
    except Exception as e:
        status_message.set(f"錯誤：無法載入國家列表 {e}")


# -----------------------------------------------------------
# 依國家載入城市資料
# -----------------------------------------------------------
def load_filtered_data():
    code = selected_country.value

    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")

        df = con.sql(f"""
            SELECT name, country, population, latitude, longitude
            FROM '{CITIES_CSV_URL}'
            WHERE country = '{code}'
            ORDER BY population DESC
            LIMIT 200;
        """).df()

        con.close()

        df["latitude"] = df["latitude"].astype(float)
        df["longitude"] = df["longitude"].astype(float)

        data_df.set(df)
        status_message.set(f"{code} 有 {len(df)} 筆城市資料")

    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())


# -----------------------------------------------------------
# 地圖元件
# -----------------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2,
            center=[20, 0],
            add_sidebar=True,
            sidebar_visible=True,
            height="900px"    # ⭐ height Allowed
        ),
        []
    )

    # ⭐ 改用 layout_style 設定寬度
    m.layout_style = {"width": "100%"}

    def update_layer():
        LAYER = "city_points"
        SOURCE = "city_source"

        # 移除舊圖層
        try:
            m.remove_layer(LAYER)
            m.remove_source(SOURCE)
        except:
            pass

        if df.empty:
            return

        features = []
        lats, lons = [], []

        for _, row in df.iterrows():
            lat = row["latitude"]
            lon = row["longitude"]
            lats.append(lat)
            lons.append(lon)

            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "name": row["name"],
                    "population": row["population"],
                },
            })

        geojson = {"type": "FeatureCollection", "features": features}

        m.add_source(SOURCE, geojson)
        m.add_layer({
            "id": LAYER,
            "type": "circle",
            "source": SOURCE,
            "paint": {
                "circle-radius": 6,
                "circle-color": "red",
                "circle-opacity": 0.9,
            },
        })

        # ⭐ 自動 zoom to bounds
        if len(lats) > 0:
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)
            m.set_bounds([[min_lon, min_lat], [max_lon, max_lat]])

    solara.use_effect(update_layer, [df])
    return m.to_solara()


# -----------------------------------------------------------
# ⭐⭐⭐ Solara Page() 必須存在
# -----------------------------------------------------------
@solara.component
def Page():

    # 初始化
    solara.use_effect(load_country_list, [])
    solara.use_effect(load_filtered_data, [selected_country.value])

    return solara.Column([

        # 國家下拉選單
        solara.Select(
            label="國家代碼",
            value=selected_country,
            values=all_countries.value
        ),

        solara.Markdown(f"**狀態：** {status_message.value}"),

        solara.Markdown("### 該國家所有城市經緯度表格"),
        solara.DataFrame(data_df.value),

        solara.Markdown("---"),

        # 地圖
        CityMap(data_df.value),
    ])
