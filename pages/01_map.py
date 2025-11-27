import solara
import duckdb
import leafmap.maplibregl as leafmap

# --- 1. 全域初始化 (資料載入、連線保持開放) ---

con = duckdb.connect(database=':memory:', read_only=False)
con.install_extension("httpfs")
con.install_extension("spatial")
con.install_extension("h3", repository="community") 
con.load_extension("httpfs")
con.load_extension("spatial")

city_url = "https://data.gishub.org/duckdb/cities.csv"
country_list_url = 'https://data.gishub.org/duckdb/countries.csv' 

# 創建城市資料表 (city_geom)
con.sql(f"""
    CREATE OR REPLACE TABLE city_geom AS
    SELECT
        *,
        ST_Point(longitude, latitude) AS geom
    FROM read_csv_auto('{city_url}');
""")

# 創建國家代碼對應表 (country_codes)
con.sql(f"""
    CREATE OR REPLACE TABLE country_codes AS
    SELECT *
    FROM read_csv_auto('{country_list_url}');
""")

# 獲取不重複的國家代碼列表
country_query_result = con.sql(f"""
    SELECT DISTINCT Alpha3_code
    FROM country_codes
    ORDER BY Alpha3_code;
""")
country_list = [row[0] for row in country_query_result.fetchall()]

# 初始化響應式變數
initial_country = country_list[0] if country_list else "TWN"
country = solara.reactive(initial_country)

# *** 新增診斷訊息響應式變數 ***
status_message = solara.reactive("請選擇一個國家...") 

# --- 2. 核心邏輯函式 (不變) ---

def create_map_instance():
    """初始化 Leafmap 地圖實例"""
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
    """根據 Alpha3_code 查詢城市經緯度 (使用 JOIN)"""
    # SQL 查詢邏輯不變
    query = f"""
        SELECT T2.latitude, T2.longitude, T2.country
        FROM country_codes AS T1
        JOIN city_geom AS T2 ON T1.country = T2.country
        WHERE T1.Alpha3_code = '{selected_alpha3_code}'
    """
    try:
        df = db_conn.sql(query).df()
    except Exception as e:
        # 如果查詢失敗，返回空數據並打印錯誤
        print(f"DuckDB Query Error: {e}")
        return {"type": "FeatureCollection", "features": []}

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
    m = solara.use_memo(create_map_instance, dependencies=[])
    
    def update_map_layer():
        selected_code = country.value
        geojson_data = get_cities_data(selected_code, con)
        
        feature_count = len(geojson_data.get('features', [])) # 獲取城市點位數
        
        m.remove_layer("selected_cities")
        
        if feature_count > 0:
            # 地圖更新邏輯
            m.add_geojson(
                geojson_data, 
                layer_name="selected_cities", 
                marker_color="red", 
                radius=5
            )
            first_coords = geojson_data['features'][0]['geometry']['coordinates']
            m.set_center(first_coords[0], first_coords[1], zoom=5)
            
            # *** 成功訊息 ***
            status_message.value = f"成功：已找到 {feature_count} 個城市點位！ (代碼: {selected_code})"
        else:
            # *** 警告訊息 ***
            status_message.value = f"警告：未找到城市點位 (代碼: {selected_code})。請檢查 'countries.csv' 和 'cities.csv' 中的國家名稱是否完全匹配。"
            
    solara.use_effect(update_map_layer, [country.value])
    
    # 3. UI 控制項 (放在地圖上方)
    controls = solara.Column(
        children=[
            solara.Select(label="Country (Alpha3_code)", value=country, values=country_list),
            solara.Markdown(f"**Selected Code**: {country.value}"),
            solara.Markdown(f"**診斷**: {status_message.value}", style={"color": "red" if "警告" in status_message.value else "green"}), # 顯示診斷訊息
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