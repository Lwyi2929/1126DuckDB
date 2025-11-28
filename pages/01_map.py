import solara
import duckdb
import leafmap.maplibregl as leafmap
import pandas as pd

CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

all_countries = solara.reactive([])
selected_country = solara.reactive("TWN")
data_df = solara.reactive(pd.DataFrame())
country_center = solara.reactive((None, None))  # 新增：國家中心 (lat, lon)
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

        all_countries.set([r[0] for r in rows])
        con.close()
        status_message.set("國家列表載入完成")
    except Exception as e:
        status_message.set(f"錯誤：無法載入國家列表 {e}")


# ----------------------------------------------------
# 載入所選國家的城市資料
# ----------------------------------------------------
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

        data_df.set(df)

        # ⭐ 新增：計算國家中心（使用平均座標）
        if len(df) > 0:
            avg_lat = df["latitude"].astype(float).mean()
            avg_lon = df["longitude"].astype(float).mean()
            country_center.set((avg_lat, avg_lon))
        else:
            country_center.set((None, None))

        status_message.set(f"{code} 共有 {len(df)} 筆城市資料")

    except Exception as e:
        status_message.set(f"錯誤：查詢資料失敗 {e}")
        data_df.set(pd.DataFrame())


# ----------------------------------------------------
# 地圖組件
# ----------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2,
            center=[20, 0],
            add_sidebar=True,
            sidebar_visible=True,
            height="800px",
        ),
        []
    )

    # ⭐ df 改 → 更新地圖點位
    def update_layer():
        LAYER = "city_points"
        SOURCE = "city_source"

        # 清除舊 layer
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
                "properties": {
                    "name": row["name"],
                    "population": row["population"]
                }
            })

        geojson = {"type": "FeatureCollection", "features": features}

        # 加入來源與圖層
        m.add_source(SOURCE, geojson)
        m.add_layer({
            "id": LAYER,
            "type": "circle",
            "source": SOURCE,
            "paint": {
                "circle-radius": 6,
                "circle-color": "red",
                "circle-opacity": 0.85,
            },
        })

        # 定位到第一筆城市
        lon, lat = features[0]["geometry"]["coordinates"]
        m.set_center(lon, lat, zoom=5)

    solara.use_effect(update_layer, [df])

    return m.to_solara()


# ----------------------------------------------------
# 主頁面
# ----------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_country_list, [])
    solara.use_effect(load_filtered_data, [selected_country.value])

    lat, lon = country_center.value

    return solara.Column([
        solara.Select(
            label="Country Code",
            value=selected_country,
            values=all_countries.value
        ),

        # ⭐ 新增：顯示國家中心經緯度
        solara.Markdown(
            f"""**國家中心座標**：  
            緯度：`{lat}`  
            經度：`{lon}`"""
            if lat is not None else
            "**尚無資料可計算國家中心**"
        ),

        solara.Markdown(f"**狀態**：{status_message.value}"),

        CityMap(data_df.value),
    ])

