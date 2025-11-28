import solara
import duckdb
import leafmap.maplibregl as leafmap
import pandas as pd

CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

all_countries = solara.reactive([])
selected_country = solara.reactive("TWN")
data_df = solara.reactive(pd.DataFrame())
status_message = solara.reactive("初始化中...")

# ----------------------------------------------------
# 載入國家列表
# ----------------------------------------------------
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

        con.close()

        all_countries.set([r[0] for r in rows])
        status_message.set("國家列表載入完成")

    except Exception as e:
        status_message.set(f"錯誤：無法載入國家列表 {e}")


# ----------------------------------------------------
# 載入城市資料
# ----------------------------------------------------
def load_filtered_data():
    code = selected_country.value
    if not code:
        data_df.set(pd.DataFrame())
        return

    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")

        df = con.sql(f"""
            SELECT name, country, population, latitude, longitude
            FROM '{CITIES_CSV_URL}'
            WHERE country = '{code}'
            ORDER BY population DESC
            LIMIT 100;
        """).df()

        con.close()

        data_df.set(df)
        status_message.set(f"已載入 {code} 資料，共 {len(df)} 筆")

    except Exception as e:
        status_message.set(f"錯誤：查詢資料失敗 {e}")
        data_df.set(pd.DataFrame())


# ----------------------------------------------------
# 地圖組件
# ----------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    # 固定建立地圖，不要隨 render 重建
    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2, center=[20, 0],
            add_sidebar=True, sidebar_visible=True, height="800px"
        ),
        []
    )

    # ⭐ 當 df 改變 → 更新地圖層
    def update_layer():
        LAYER = "city_points"
        SOURCE = "city_source"

        # 清除前一次圖層
        try:
            m.remove_layer(LAYER)
            m.remove_source(SOURCE)
        except:
            pass

        if df.empty:
            return

        # 建立 GeoJSON
        features = []
        for _, row in df.iterrows():
            try:
                lon = float(row["longitude"])
                lat = float(row["latitude"])
            except:
                continue

            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {"name": row["name"]}
            })

        geojson = {"type": "FeatureCollection", "features": features}

        m.add_source(SOURCE, geojson)
        m.add_layer({
            "id": LAYER,
            "type": "circle",
            "source": SOURCE,
            "paint": {
                "circle-radius": 5,
                "circle-color": "red",
            },
        })

        # 重新定位地圖
        lon, lat = features[0]["geometry"]["coordinates"]
        m.set_center(lon, lat, zoom=5)

    # ⭐ 正確依賴：df 變 → 更新地圖
    solara.use_effect(update_layer, [df])

    return m.to_solara()


# ----------------------------------------------------
# 主頁面
# ----------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_country_list, [])
    solara.use_effect(load_filtered_data, [selected_country.value])

    return solara.Column([
        solara.Select(
            label="Country Code",
            value=selected_country,
            values=all_countries.value
        ),
        solara.Markdown(f"**狀態**：{status_message.value}"),
        CityMap(data_df.value),
    ])
