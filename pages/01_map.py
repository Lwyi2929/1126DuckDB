import solara
import duckdb
import leafmap.maplibregl as leafmap

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


# --- 2. 核心邏輯函式 ---

def create_map_instance():
    """初始化 Leafmap 地圖實例 (只執行一次)"""
    m = leafmap.Map(
        add_sidebar=True,
        sidebar_visible=True,
        height="800px",
    )
    # 保持用戶的配置
    m.add_basemap("Esri.WorldImagery", visible=False)
    m.add_draw_control(controls=["polygon", "trash"])
    return m

def get_cities_data(selected_alpha3_code, db_conn):
    """根據 Alpha3_code 直接過濾 city_geom 表並返回 GeoJSON 數據。"""
    query = f"""
        SELECT latitude, longitude
        FROM city_geom
        WHERE country = '{selected_alpha3_code}' 
    """
    df = db_conn.sql(query).df()
    
    features = []
    for index, row in df.iterrows():
        features.append(
            {
                "type": "Feature",
                # GeoJSON 順序是 [經度, 緯度]
                "geometry": {"type": "Point", "coordinates": [row['longitude'], row['latitude']]},
                "properties": {}, # 移除不必要的屬性，進一步簡化 GeoJSON
            }
        )
    return {"type": "FeatureCollection", "features": features}


# --- 3. Solara 組件 (響應式地圖更新) ---

@solara.component
def Page():
    # 使用 memo 確保地圖只創建一次
    m = solara.use_memo(create_map_instance, dependencies=[])
    
    # 圖層 ID 變數 (用於確保移除和添加操作是對應的)
    LAYER_ID = "selected_cities"

    def update_map_layer():
        selected_code = country.value
        geojson_data = get_cities_data(selected_code, con)
        feature_count = len(geojson_data.get('features', []))
        
        # 1. 清除舊圖層：使用 try-except 確保即使圖層不存在也不會中斷
        try:
             # 移除舊圖層 (使用我們定義的 ID)
             m.remove_layer(LAYER_ID)
             # 可能也需要移除底層的數據源
             m.remove_source(LAYER_ID) 
        except Exception:
             pass 
        
        if feature_count > 0:
            # 2. 修正核心：只傳遞 GeoJSON 數據，並使用 Leafmap 的點位/標記功能
            # 由於 add_geojson 驗證嚴格，我們直接使用 add_markers 處理點位列表更安全
            
            # 從 GeoJSON 中提取 [lon, lat] 列表
            points = [f["geometry"]["coordinates"] for f in geojson_data["features"]]

            # *** 使用 add_points 或 add_markers (更穩定且允許樣式) ***
            m.add_markers(
                points, 
                layer_name=LAYER_ID,          # 設置 ID
                color="red",                  # 設置顏色
                popup_text=selected_code      # 可選：添加彈出文本
            )
            
            # 定位到選定國家的第一個點位
            first_coords = points[0] # [lon, lat]
            m.set_center(first_coords[0], first_coords[1], zoom=5)
            
            status_message.value = f"成功：已找到 {feature_count} 個城市點位！ (代碼: {selected_code})"
        else:
            status_message.value = f"警告：未找到城市點位 (代碼: {selected_code})。"
            
    solara.use_effect(update_map_layer, [country.value])
    
    # 3. UI 控制項 (放在地圖上方)
    controls = solara.Column(
        children=[
            solara.Select(label="Country (Alpha3_code)", value=country, values=country_list),
            solara.Markdown(f"**Selected Code**: {country.value}"),
            solara.Markdown(f"**診斷**: {status_message.value}", style={"color": "red" if "警告" in status_message.value else "green"}), 
        ]
    )

    map_component = m.to_solara()

    # 4. 組合：垂直堆疊控制項和地圖
    return solara.Column(
        children=[
            controls,
            map_component
        ]
    )