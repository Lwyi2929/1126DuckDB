import solara
import duckdb
import leafmap.maplibregl as leafmap
import pandas as pd

# --- 1. 全域初始化 (資料準備，只執行一次) ---

con = duckdb.connect(database=':memory:', read_only=False)
con.install_extension("httpfs")
con.install_extension("spatial")
con.load_extension("httpfs")
con.load_extension("spatial")

city_url = "https://data.gishub.org/duckdb/cities.csv" 

# 創建城市資料表 (city_geom)
con.sql(f"""
    CREATE OR REPLACE TABLE city_geom AS
    SELECT
        *,
        ST_Point(longitude, latitude) AS geom
    FROM read_csv_auto('{city_url}');
""")

# 獲取不重複的國家代碼列表
country_query_result = con.sql("SELECT DISTINCT country FROM city_geom ORDER BY country;")
country_list = [row[0] for row in country_query_result.fetchall()]

# 初始化響應式變數
initial_country = country_list[0] if country_list else "TWN"
country = solara.reactive(initial_country)
status_message = solara.reactive("請選擇一個國家...") 


# --- 2. 輔助函式 ---

def create_map_instance():
    """初始化 Leafmap 地圖實例 (只執行一次)"""
    m = leafmap.Map(
        zoom=2,
        add_sidebar=True,
        sidebar_visible=True,
        height="800px",
    )
    m.add_basemap("Esri.WorldImagery", visible=False)
    m.add_draw_control(controls=["polygon", "trash"])
    return m

def fetch_city_data(selected_code):
    """從 DuckDB 獲取選定國家的城市數據，返回 DataFrame。"""
    query = f"""
        SELECT latitude, longitude, country, name, population
        FROM city_geom
        WHERE country = '{selected_code}' 
    """
    try:
        df = con.sql(query).df()
        return df
    except Exception as e:
        print(f"DuckDB Fetch Error: {e}")
        return pd.DataFrame()

# --- 3. Solara 組件 (響應式地圖更新) ---

@solara.component
def Page():
    # 1. 地圖實例: 保持使用 use_memo 確保地圖只創建一次
    m = solara.use_memo(create_map_instance, dependencies=[])
    
    # 2. 獲取數據資源：當 country.value 改變時，自動重新執行 fetch_city_data
    city_df_resource = solara.use_resource(fetch_city_data, kwargs={"selected_code": country.value})
    
    # 3. 響應式效果: 監聽數據資源狀態 (city_df_resource.value) 的變化
    def update_map_layer():
        # 如果數據正在載入，則跳過更新
        if city_df_resource.state == solara.ResourceState.LOADING:
            status_message.value = "正在載入數據..."
            return

        df = city_df_resource.value # 獲取 DataFrame
        feature_count = len(df)
        
        # --- 清除舊圖層 (Leafmap/MapLibreGL 核心邏輯) ---
        # 由於 add_geojson 沒有 ID，我們需要手動移除 'geojson' 相關的源和圖層
        LAYER_ID = "geojson_layer_0" # Leafmap 預設會為 add_geojson 創建一個圖層
        SOURCE_ID = "geojson_source_0"

        try:
             m.remove_layer(LAYER_ID)
             m.remove_source(SOURCE_ID)
        except Exception:
             pass 

        if feature_count > 0:
            
            # --- 數據轉換 (參考您提供的範例) ---
            features = []
            for index, row in df.iterrows():
                try:
                    population = int(row["population"])
                except (ValueError, TypeError):
                    population = None
                            
                features.append({
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [row["longitude"], row["latitude"]]}, # [lon, lat] 順序
                    "properties": {
                        "name": row["name"],
                        "country": row["country"],
                        "population": population
                    }
                })
            geojson = {"type": "FeatureCollection", "features": features}

            # --- 繪製點位 (參考您提供的範例) ---
            # 關鍵：只傳遞 geojson 數據，讓 Leafmap 自動處理樣式/ID
            m.add_geojson(geojson)
            
            # 定位到選定國家的第一個城市
            center_lon, center_lat = features[0]["geometry"]["coordinates"]
            m.set_center(center_lon, center_lat, zoom=5)
            
            status_message.value = f"成功：已找到 {feature_count} 個城市點位！ (代碼: {country.value})"
        else:
            status_message.value = f"警告：未找到城市點位 (代碼: {country.value})。"
            
    # 監聽數據資源的變化，觸發地圖更新
    solara.use_effect(update_map_layer, [city_df_resource.state, city_df_resource.value])
    
    # 4. UI 控制項
    controls = solara.Column(
        children=[
            solara.Select(label="Country (Alpha3_code)", value=country, values=country_list),
            solara.Markdown(f"**Selected Code**: {country.value}"),
            solara.Markdown(f"**診斷**: {status_message.value}", style={"color": "red" if "警告" in status_message.value else "green"}), 
        ]
    )

    map_component = m.to_solara()

    # 5. 組合：垂直堆疊控制項和地圖
    return solara.Column(
        children=[
            controls,
            map_component
        ]
    )