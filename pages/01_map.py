import solara
import duckdb
import leafmap.maplibregl as leafmap
import pandas as pd

# ----------------------------------------------------
# 0. 常量
# ----------------------------------------------------
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

# ----------------------------------------------------
# 1. 狀態管理 (Reactive Variables)
# ----------------------------------------------------
all_countries = solara.reactive([])        # 所有不重複的國家代碼列表
selected_country = solara.reactive("TWN")  # 預設為 'TWN'
data_df = solara.reactive(pd.DataFrame()) # 當前城市的數據 DataFrame
status_message = solara.reactive("初始化中...")

# ----------------------------------------------------
# 2. 數據獲取邏輯
# ----------------------------------------------------

# A. 載入所有國家清單 (應用程式啟動時執行一次)
def load_country_list():
    status_message.set("正在載入國家列表...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        
        result = con.sql(f"""
            SELECT DISTINCT country 
            FROM '{CITIES_CSV_URL}'
            ORDER BY country;
        """).fetchall()
        
        country_list = [row[0] for row in result]
        all_countries.set(country_list)
        
        if not country_list:
             status_message.set("警告：國家列表為空。")
             
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入國家列表失敗 ({e})")

# B. 根據選中的國家篩選城市數據
def load_filtered_data():
    country_name = selected_country.value
    if not country_name:
        data_df.set(pd.DataFrame())
        return 
        
    status_message.set(f"正在查詢 {country_name} 的城市數據...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE country = '{country_name}'
        ORDER BY population DESC
        LIMIT 100;
        """
        df_result = con.sql(sql_query).df()
        data_df.set(df_result)
                
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：執行查詢失敗 ({e})")
        data_df.set(pd.DataFrame())

# ----------------------------------------------------
# 3. 視覺化組件
# ----------------------------------------------------

@solara.component
def CityMap(df: pd.DataFrame):
    
    # 使用 use_memo 確保地圖實例只創建一次
    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2, center=[40.7, -74.0],
            add_sidebar=True,
            sidebar_visible=True,
            height="800px",
        ), dependencies=[]
    )
    # 設置底圖和控制項
    m.add_basemap("Esri.WorldImagery", visible=False)
    m.add_draw_control(controls=["polygon", "trash"])

    # --- 響應式：當 DF 改變時，更新地圖圖層 ---
    def update_map_layer():
        LAYER_ID = "selected_cities_points"
        SOURCE_ID = "cities_data_source"

        # 1. 刪除多餘圖層：清除舊圖層和來源
        try:
             m.remove_layer(LAYER_ID)
             m.remove_source(SOURCE_ID)
        except Exception:
             pass 
        
        if df.empty:
            status_message.set(f"警告：未找到城市點位 (代碼: {selected_country.value})。")
            return
            
        # 2. 數據轉換為 GeoJSON 字典 (已加入顯式類型轉換，解決渲染問題)
        features = []
        center_coords = None
        valid_feature_count = 0
        for index, row in df.iterrows():
            try:
                # 確保經緯度是標準 Python float 類型
                lon, lat = float(row["longitude"]), float(row["latitude"])
                population = int(row["population"]) if pd.notna(row.get("population")) else None
            except Exception:
                continue # 跳過無效行
            
            if center_coords is None:
                center_coords = [lon, lat]
                
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]}, # [lon, lat] 順序
                "properties": {
                    "name": row["name"],
                    "country": row["country"],
                    "population": population
                }
            })
            valid_feature_count += 1

        if not features:
            status_message.set(f"警告：所有城市數據轉換失敗，未繪製點位 (代碼: {selected_country.value})。")
            return
        
        # 3. 依其表格在地圖上顯示點位：添加新的數據源和圖層
        m.add_source(SOURCE_ID, geojson) 
        m.add_layer(
            {
                "id": LAYER_ID,
                "source": SOURCE_ID,  
                "type": "circle",     
                "paint": {
                    "circle-radius": 5,
                    "circle-color": "red",
                    "circle-opacity": 0.8
                }
            }
        )
        
        # 4. 定位地圖
        if center_coords:
            m.set_center(center_coords[0], center_coords[1], zoom=5)
            
        # 5. 更新狀態訊息
        status_message.set(f"成功：已找到 {valid_feature_count} 個城市點位！ (代碼: {selected_country.value})")

    solara.use_effect(update_map_layer, [df.values.tolist()]) 
    
    # 返回地圖組件
    return m.to_solara()

# ----------------------------------------------------
# 4. 主頁面組件 (修正後的版本)
# ----------------------------------------------------

@solara.component
def Page():
    # A. 應用程式啟動時，載入國家清單
    solara.use_effect(load_country_list, []) 
    
    # B. 當 selected_country 改變時，載入篩選後的數據
    solara.use_effect(load_filtered_data, [selected_country.value])
    
    # 檢查國家列表是否載入。如果沒有，顯示載入訊息。
    if not all_countries.value:
        return solara.Info("正在載入國家列表，請稍候...")

    # 1. UI 控制項
    controls = solara.Column(
        children=[
            solara.Select(label="Country (Alpha3_code)", value=selected_country, values=all_countries.value),
            solara.Markdown(f"**Selected Code**: {selected_country.value}"),
            solara.Markdown(
                f"**診斷**: {status_message.value}", 
                style={"color": "red" if "警告" in status_message.value or "錯誤" in status_message.value else "green"}
            ), 
        ]
    )

    # 2. 城市數據表格 (僅在數據非空時顯示)
    city_table = None
    if not data_df.value.empty:
        # 選擇需要的欄位並重新命名
        df_for_table = data_df.value[['name', 'country', 'latitude', 'longitude']].rename(
            columns={'name': '城市名稱(代碼)', 'country': '代碼', 'latitude': '緯度', 'longitude': '經度'}
        )
        city_table = solara.Column(
            children=[
                solara.Markdown("### 城市清單與座標詳情"),
                solara.DataTable(df_for_table)
            ]
        )
        
    # 3. 渲染地圖組件
    map_display = CityMap(df=data_df.value)

    # 4. 組合：控制項 -> 表格 -> 地圖
    return solara.Column(
        children=[
            controls,
            city_table,  # 顯示表格 (如果存在數據)
            map_display
        ]
    )