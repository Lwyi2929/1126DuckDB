import solara
import duckdb
import pandas as pd
import leafmap.maplibregl as leafmap
import numpy as np

# ----------------------------------------------------
# 0. 常量與預設值
# ----------------------------------------------------
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'
# 確保這些變數存在，即使它們不再用於全域範圍計算
MIN_POP_DEFAULT = 100000 
MAX_POP_SLIDER = 20_000_000 

# ----------------------------------------------------
# 1. 狀態管理 (Reactive Variables) - 採用新的變數結構
# ----------------------------------------------------
all_countries = solara.reactive([])        # 國家清單 (用於 Select)
selected_country = solara.reactive("TWN")  # 當前選定的國家
population_threshold = solara.reactive(1_000_000) # ⭐ 新增：人口門檻
data_df = solara.reactive(pd.DataFrame()) 
status_message = solara.reactive("初始化中...")


# ----------------------------------------------------
# 2. 數據獲取邏輯 (採用用戶的 Country + Threshold 邏輯)
# ----------------------------------------------------

# A. 載入國家清單 (取代舊的 load_global_pop_bounds)
def load_country_list():
    status_message.set("正在載入國家列表...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        
        result = con.sql(f"SELECT DISTINCT country FROM '{CITIES_CSV_URL}' ORDER BY country;").fetchall()
        country_list = [row[0] for row in result]
        all_countries.set(country_list)
        
        # 預設選 TWN，如果沒有再選 USA (保持使用者偏好)
        if "TWN" in country_list:
             selected_country.set("TWN")
        elif "USA" in country_list:
             selected_country.set("USA")
        elif country_list:
             selected_country.set(country_list[0])
        
        status_message.set("國家列表載入完成")
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入國家列表失敗 ({e})")

# B. 載入該國家 + 人口門檻的城市 (採用用戶的篩選邏輯)
def load_filtered_data():
    country_name = selected_country.value
    threshold = population_threshold.value
    
    if not country_name:
        data_df.set(pd.DataFrame()); return
        
    status_message.set(f"正在查詢 {country_name} (人口 ≥ {threshold:,})...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        
        df_result = con.sql(f"""
            SELECT name, country, population, latitude, longitude
            FROM '{CITIES_CSV_URL}'
            WHERE country = '{country_name}'
              AND population >= {threshold}
            ORDER BY population DESC
            LIMIT 200;
        """).df()
        
        # 確保數據類型正確
        df_result["latitude"] = df_result["latitude"].astype(float)
        df_result["longitude"] = df_result["longitude"].astype(float)
        
        data_df.set(df_result)
        status_message.set(f"成功：載入 {country_name} 的 {len(df_result)} 筆城市資料 (≥ {threshold:,})")
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 ({e})")
        data_df.set(pd.DataFrame())

# -----------------------------------------------------------
# 3. 視覺化組件 (CityMap) - 採用穩定的繪圖邏輯
# -----------------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):
    if df.empty:
        return solara.Info("沒有符合人口門檻的城市")

    # 地圖中心點設為人口最大的城市
    center = [df['latitude'].iloc[0], df['longitude'].iloc[0]]

    m = leafmap.Map(
        center=center,
        zoom=4,
        add_sidebar=True,
        height="600px"
    )
    m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id)

    # 轉成 GeoJSON
    features = []
    for _, row in df.iterrows():
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [row["longitude"], row["latitude"]]
            },
            "properties": {
                "name": row["name"],
                "population": int(row["population"])
            }
        })

    geojson = {"type": "FeatureCollection", "features": features}
    m.add_geojson(geojson)

    return m.to_solara()

# -----------------------------------------------------------
# 4. Solara 主頁面
# -----------------------------------------------------------
# 4. 主頁面組件
# -----------------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_global_pop_bounds, []) 
    solara.use_effect(load_filtered_data, [min_pop_value.value, max_pop_value.value])

    min_available_pop, max_available_pop = country_pop_bounds.value
    
    # 檢查是否仍在載入初始邊界
    if max_available_pop == MAX_POP_SLIDER and status_message.value.startswith("正在載入"):
         return solara.Info("正在載入全域人口邊界...")

    # 城市表格
    city_table = None
    df = data_df.value
    if not df.empty:
        # ... (表格創建邏輯) ...
        df_for_table = df[['name', 'country', 'latitude', 'longitude', 'population']].rename(
            columns={'name': '城市名稱', 'country': '代碼', 'latitude': '緯度', 'longitude': '經度', 'population': '人口'}
        )
        city_table = solara.Column([solara.Markdown("### 城市清單與座標詳情"), solara.DataTable(df_for_table)])
    
    
    # 組合所有元件的列表
    main_components = [
        solara.Card(title="城市數據篩選與狀態", elevation=2),

        # 1. 控制項
        solara.SliderInt(label=f"最低人口 (人): {min_pop_value.value:,}", value=min_pop_value, min=min_available_pop, max=max_available_pop, step=50000),
        solara.SliderInt(label=f"最高人口 (人): {max_pop_value.value:,}", value=max_pop_value, min=min_available_pop, max=max_available_pop, step=50000),
        
        solara.Markdown(f"**狀態：** {status_message.value}"),
        solara.Markdown("---"),
        
        # 2. 地圖
        CityMap(data_df.value),
        
        # 3. 表格 (只有當 city_table 被賦值時才會被包含)
        city_table, 
    ]
    
    # ⭐ 關鍵修正：確保在傳入 children 列表時，所有 None 值被過濾
    return solara.Column([item for item in main_components if item is not None])
