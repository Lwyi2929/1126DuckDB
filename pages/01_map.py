import solara
import duckdb
import leafmap.maplibregl as leafmap

# --- 1. 全域初始化 (資料載入、連線保持開放) ---

# 設置 DuckDB 連線並保持開放 (重要：確保 city_geom 表格存在於記憶體中)
con = duckdb.connect(database=':memory:', read_only=False)
con.install_extension("httpfs")
con.install_extension("spatial")
con.load_extension("httpfs")
con.load_extension("spatial")

# 1. 定義資料 URL
city_url = "https://data.gishub.org/duckdb/cities.csv"
country_list_url = 'https://data.gishub.org/duckdb/countries.csv'

# 2. 創建城市資料表 (物化 city_geom 表格)
con.sql(f"""
    CREATE OR REPLACE TABLE city_geom AS
    SELECT
        *,
        ST_Point(longitude, latitude) AS geom
    FROM read_csv_auto('{city_url}');
""")

# 3. 獲取不重複的國家列表
country_query_result = con.sql(f"""
    SELECT DISTINCT country
    FROM read_csv_auto('{country_list_url}')
    ORDER BY country;
""")
country_list = [row[0] for row in country_query_result.fetchall()]

# 4. 初始化響應式變數
initial_country = country_list[0] if country_list else "Taiwan"
country = solara.reactive(initial_country)

# --- 2. 核心邏輯函式 ---

def create_map_instance():
    """初始化 Leafmap 地圖實例 (只執行一次)"""
    # 移除 create_map 中與 Solara 側邊欄衝突的參數
    m = leafmap.Map(
        layer_manager_expanded=False,
        height="800px",
    )
    m.add_basemap("Esri.WorldImagery")
    return m

def get_cities_data(country_name, db_conn):
    """從 DuckDB 查詢指定國家的城市經緯度"""
    query = f"""
        SELECT latitude, longitude
        FROM city_geom
        WHERE country = '{country_name}'
    """
    df = db_conn.sql(query).df()
    
    # 轉換為 Leafmap 易於處理的 GeoJSON 格式
    features = []
    for index, row in df.iterrows():
        # GeoJSON 座標順序為 [經度, 緯度] (Longitude, Latitude)
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [row['longitude'], row['latitude']]},
                "properties": {"country": country_name},
            }
        )
    return {"type": "FeatureCollection", "features": features}


# --- 3. Solara 組件 (響應式地圖更新) ---

@solara.component
def Page():
    # 1. 地圖實例：保持使用 use_memo 確保地圖只創建一次
    m = solara.use_memo(create_map_instance, dependencies=[]) # 假設 create_map_instance 仍是初始化函式

    # 2. 響應式效果：必須保留此邏輯來監聽並更新地圖圖層
    def update_map_layer():
        selected_country = country.value
        geojson_data = get_cities_data(selected_country, con)
        
        m.remove_layer("selected_cities")
        if geojson_data['features']:
            m.add_geojson(
                geojson_data, 
                layer_name="selected_cities", 
                marker_color="red", 
                radius=5
            )
            first_coords = geojson_data['features'][0]['geometry']['coordinates']
            m.set_center(first_coords[0], first_coords[1], zoom=5)
            
    # 觸發更新
    solara.use_effect(update_map_layer, [country.value])
    
    # 3. 側邊欄 (UI 控制項)
    with solara.Sidebar():
        solara.Select(label="Country", value=country, values=country_list)
        solara.Markdown(f"**Selected**: {country.value}")

    # 4. 渲染地圖：使用 to_solara() 替換 leafmap.Map(m)
    return m.to_solara()