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
@solara.component
def Page():

    # 執行數據載入邏輯
    solara.use_effect(load_country_list, [])
    # ⭐ 監聽國家和人口門檻的變化
    solara.use_effect(load_filtered_data, [selected_country.value, population_threshold.value])

    if not all_countries.value and status_message.value.startswith("正在載入"):
         return solara.Info("正在載入國家清單...")

    # 城市表格 (若超過 300 筆則限制顯示)
    city_table = None
    df = data_df.value
    if not df.empty:
        num_records = len(df)
        warning_message = None
        
        if num_records > 300:
            df_to_display = df.head(100)
            warning_message = solara.Warning(f"資料量超過 300 筆 ({num_records} 筆)。表格僅顯示前 100 筆記錄。", dense=True)
        else:
            df_to_display = df

        df_for_table = df_to_display[['name', 'country', 'latitude', 'longitude', 'population']].rename(
            columns={'name': '城市名稱', 'country': '代碼', 'latitude': '緯度', 'longitude': '經度', 'population': '人口'}
        )
        
        children = [
            solara.Markdown("### 城市清單與座標詳情"),
            warning_message,
            solara.DataTable(df_for_table)
        ]
        city_table = solara.Column([child for child in children if child is not None])
    
    # 組合頁面佈局
    return solara.Column([

        solara.Card(title="城市數據篩選器", elevation=2, children=[
            solara.Select(
                label="選擇國家",
                value=selected_country,
                values=all_countries.value
            ),
            # ⭐ 人口門檻 Slider
            solara.SliderInt(
                label="人口下限",
                value=population_threshold,
                min=0,
                max=MAX_POP_SLIDER,
                step=100_000 # 步進 10 萬
            ),
            solara.Markdown(f"目前人口門檻：**{population_threshold.value:,}**"),
            solara.Markdown(f"**狀態：** {status_message.value}"),
        ]),
        
        solara.Markdown("---"),
        
        CityMap(data_df.value),
        
        city_table,
    ])
