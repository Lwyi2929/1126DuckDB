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

        # 確保經緯度為 float 類型，避免 GeoJSON 序列化錯誤
        df["latitude"] = df["latitude"].astype(float)
        df["longitude"] = df["longitude"].astype(float)

        data_df.set(df)
        status_message.set(f"{code} 已載入 {len(df)} 筆城市資料")

    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())


# -----------------------------------------------------------
## 🗺️ 地圖元件
# -----------------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2,
            center=[0, 0],  # 修正初始中心點
            add_sidebar=True,
            sidebar_visible=True,
            # 移除 height/width 參數以避免 Pydantic 驗證錯誤
        ),
        []
    )
    
    # 修正：將底圖和繪圖控制移到 use_memo 內部，確保只執行一次
    # 如果 Leafmap Map 物件已存在，則跳過設定，否則設定
    if not hasattr(m, '_initialized_base_layers'):
        m.add_basemap("Esri.WorldImagery") 
        m.add_draw_control(controls=["polygon", "trash"])
        m._initialized_base_layers = True
    
    # ⭐ 設置地圖元件的佈局大小
    m.layout.height = "900px" 

    def update_layer():
        LAYER = "city_points"
        SOURCE = "city_source"

        # 移除舊圖層
        try:
            m.remove_layer(LAYER)
            m.remove_source(SOURCE)
        except Exception:
            pass

        if df.empty:
            return

        features = []
        lats, lons = [], []
        
        # 轉換 GeoJSON
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

        # 添加數據源和圖層
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

        # 自動 zoom to bounds
        if len(lats) > 0:
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)
            # set_bounds 接受 [[min_lon, min_lat], [max_lon, max_lat]]
            m.set_bounds([[min_lon, min_lat], [max_lon, max_lat]])

    # 監聽 df 內容的變化
    solara.use_effect(update_layer, [df]) 
    return m.to_solara()


# -----------------------------------------------------------
## 📑 Solara Page() 
# -----------------------------------------------------------
@solara.component
def Page():

    # 初始化
    solara.use_effect(load_country_list, [])
    # 監聽下拉選單變化
    solara.use_effect(load_filtered_data, [selected_country.value])

    return solara.Column([

        # 國家下拉選單
        solara.Select(
            label="國家代碼",
            value=selected_country,
            values=all_countries.value
        ),

        solara.Markdown(f"**狀態：** {status_message.value}"),

        # 城市表格
        solara.Markdown("### 城市清單與座標表格"),
        solara.DataFrame(data_df.value),

        solara.Markdown("---"),

        # 地圖
        CityMap(data_df.value),
    ])