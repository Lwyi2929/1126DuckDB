import solara
import duckdb
import leafmap.maplibregl as leafmap

# --- 1. 全域初始化 (資料載入、連線保持開放) ---

# 設置 DuckDB 連線並保持開放
con = duckdb.connect(database=':memory:', read_only=False)

# 整合用戶提供的 DuckDB 擴展載入
con.install_extension("httpfs")
con.install_extension("spatial")
# 由於 Leafmap 需要，保留 H3 擴展
con.install_extension("h3", repository="community") 
con.load_extension("httpfs")
con.load_extension("spatial")


# 1. 定義資料 URL
city_url = "https://data.gishub.org/duckdb/cities.csv"
country_list_url = 'https://data.gishub.org/duckdb/countries.csv' 


# 2a. 創建城市資料表 (city_geom) - 使用 'country' 欄位
con.sql(f"""
    CREATE OR REPLACE TABLE city_geom AS
    SELECT
        *,
        ST_Point(longitude, latitude) AS geom
    FROM read_csv_auto('{city_url}');
""")

# 2b. 創建國家代碼對應表 (country_codes) - 包含 'Alpha3_code' 欄位
con.sql(f"""
    CREATE OR REPLACE TABLE country_codes AS
    SELECT *
    FROM read_csv_auto('{country_list_url}');
""")


# 3. 獲取不重複的國家代碼列表 (用於下拉選單)
country_query_result = con.sql(f"""
    SELECT DISTINCT Alpha3_code
    FROM country_codes
    ORDER BY Alpha3_code;
""")
country_list = [row[0] for row in country_query_result.fetchall()]

# 4. 初始化響應式變數
initial_country = country_list[0] if country_list else "TWN"
country = solara.reactive(initial_country)

# --- 2. 核心邏輯函式 ---

# 整合用戶提供的 Leafmap 初始化功能
def create_map_instance():
    """初始化 Leafmap 地圖實例 (只執行一次)"""
    m = leafmap.Map(
        add_sidebar=True,
        add_floating_sidebar=False,
        sidebar_visible=True,
        layer_manager_expanded=False,
        height="800px",
    )
    # 整合用戶提供的底圖和繪圖控制
    m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id, visible=False)
    m.add_draw_control(controls=["polygon", "trash"])
    
    # 移除原 create_map 中重複的 DuckDB 連線邏輯
    
    return m

def get_cities_data(selected_alpha3_code, db_conn):
    """
    根據選定的 Alpha3_code 查詢城市經緯度。
    使用 JOIN 查找對應的完整國家名稱。
    """
    # 查詢邏輯不變：使用 JOIN 找到對應的完整國家名稱
    query = f"""
        SELECT T2.latitude, T2.longitude, T2.country
        FROM country_codes AS T1
        JOIN city_geom AS T2 ON T1.country = T2.country
        WHERE T1.Alpha3_code = '{selected_alpha3_code}'
    """
    df = db_conn.sql(query).df()
    
    # 轉換為 Leafmap 易於處理的 GeoJSON 格式
    features = []
    for index, row in df.iterrows():
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [row['longitude'], row['latitude']]},
                "properties": {"country": row['country']},
            }
        )
    return {"type": "FeatureCollection", "features": features}


# --- 3. Solara 組件 (響應式地圖更新) ---

@solara.component
def Page():
    # 1. 地圖實例: 使用 use_memo 確保地圖只創建一次
    m = solara.use_memo(create_map_instance, dependencies=[])
    
    # 2. 響應式效果: 監聽 country.value (Alpha3_code) 的變化
    def update_map_layer():
        selected_code = country.value
        geojson_data = get_cities_data(selected_code, con)
        
        m.remove_layer("selected_cities")

        if geojson_data['features']:
            m.add_geojson(
                geojson_data, 
                layer_name="selected_cities", 
                marker_color="red", 
                radius=5
            )
            
            # 定位到選定國家的第一個城市
            first_coords = geojson_data['features'][0]['geometry']['coordinates']
            m.set_center(first_coords[0], first_coords[1], zoom=5)
            
    # 當 country.value 改變時，觸發更新
    solara.use_effect(update_map_layer, [country.value])
    
    # 3. 側邊欄 (UI 控制項)
    # 由於 Leafmap 已經設置了 add_sidebar=True，這裡使用 solara.Sidebar() 會將 UI 元素放入 Leafmap 的側邊欄中
    with solara.Sidebar():
        solara.Select(label="Country (Alpha3_code)", value=country, values=country_list)
        solara.Markdown(f"**Selected Code**: {country.value}")

    # 4. 渲染地圖
    # 使用 m.to_solara() 渲染地圖實例
    return m.to_solara()
