import solara
import duckdb
import leafmap.maplibregl as leafmap

# --- 1. 全域初始化 (資料載入、連線保持開放) ---

# 設置 DuckDB 連線並保持開放
con = duckdb.connect(database=':memory:', read_only=False)

# 載入擴展 (只保留 httpfs 和 spatial，移除未使用的 h3)
con.install_extension("httpfs")
con.install_extension("spatial")
con.load_extension("httpfs")
con.load_extension("spatial")

# 1. 定義資料 URL
city_url = "https://data.gishub.org/duckdb/cities.csv" 

# 2. 創建城市資料表 (city_geom)
con.sql(f"""
    CREATE OR REPLACE TABLE city_geom AS
    SELECT
        *,
        ST_Point(longitude, latitude) AS geom
    FROM read_csv_auto('{city_url}');
""")

# 3. 獲取不重複的國家代碼列表
country_query_result = con.sql(f"""
    SELECT DISTINCT country 
    FROM city_geom
    ORDER BY country;
""")
country_list = [row[0] for row in country_query_result.fetchall()]

# 4. 初始化響應式變數
initial_country = country_list[0] if country_list else "TWN"
country = solara.reactive(initial_country)
status_message = solara.reactive("請選擇一個國家...") 


# --- 2. 核心邏輯函式 ---

def create_map_instance():
    """初始化 Leafmap 地圖實例 (只執行一次)"""
    m = leafmap.Map(
        add_sidebar=True,
        add_floating_sidebar=False,
        sidebar_visible=True,
        layer_manager_expanded=False,
        height="800px",
    )
    m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id, visible=False)
    m.add_draw_control(controls=["polygon", "trash"])
    return m

def get_cities_data(selected_alpha3_code, db_conn):
    """根據選定的 Alpha3_code 直接過濾 city_geom 表。"""
    query = f"""
        SELECT latitude, longitude, country
        FROM city_geom
        WHERE country = '{selected_alpha3_code}' 
    """
    
    try:
        df = db_conn.sql(query).df()
    except Exception as e:
        print(f"DuckDB Query Error: {e}")
        return {"type": "FeatureCollection", "features": []}

    features = []
    for index, row in df.iterrows():
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [row['longitude'], row['latitude']]},
                "properties": {"country_code": row['country']},
            }
        )
    return {"type": "FeatureCollection", "features": features}


# --- 3. Solara 組件 (響應式地圖更新) ---

@solara.component
def Page():
    m = solara.use_memo(create_map_instance, dependencies=[])
    
    def update_map_layer():
        selected_code = country.value
        geojson_data = get_cities_data(selected_code, con)
        
        feature_count = len(geojson_data.get('features', []))
        
        m.remove_layer("selected_cities")
        
        if feature_count > 0:
            # 修正後的 add_geojson 樣式設置 (解決 Pydantic 驗證錯誤)
            m.add_geojson(
                geojson_data, 
                style={
                    "type": "circle",
                    "paint": {
                        "circle-radius": 5,
                        "circle-color": "blue",
                        "circle-opacity": 0.8
                    }
                },
                style_options={"id": "selected_cities"}
            )
            
            # 定位到選定國家的第一個城市
            first_coords = geojson_data['features'][0]['geometry']['coordinates']
            m.set_center(first_coords[0], first_coords[1], zoom=5)
            
            status_message.value = f"成功：已找到 {feature_count} 個城市點位！ (代碼: {selected_code})"
        else:
            status_message.value = f"警告：未找到城市點位 (代碼: {selected_code})。該國家可能沒有城市數據或資料有誤。"
            
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