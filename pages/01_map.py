import solara
import duckdb
import pandas as pd
# import plotly.express as px # 暫時不需要 Plotly，除非要展示圖表
import leafmap.maplibregl as leafmap
import numpy as np

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
status_message = solara.reactive("初始化中...") # 保留診斷狀態變數

# ----------------------------------------------------
# 2. 數據獲取邏輯 (採用範例結構)
# ----------------------------------------------------

# A. 載入所有國家清單
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
        
        all_countries.set([r[0] for r in result])
        
        # 保持 TWN 預設值，如果清單為空則顯示警告
        if not all_countries.value:
             status_message.set("警告：國家列表為空。")
        else:
             status_message.set("國家列表載入完成")
             
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入國家列表失敗 ({e})")

# B. 根據選中的國家篩選城市數據
def load_filtered_data():
    code = selected_country.value
    if not code:
        data_df.set(pd.DataFrame())
        return 
        
    status_message.set(f"正在查詢 {code} 的城市數據...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE country = '{code}'
        ORDER BY population DESC
        LIMIT 200;
        """
        df_result = con.sql(sql_query).df()
        
        # 修正：確保經緯度為 float 類型，避免 GeoJSON 序列化錯誤
        df_result["latitude"] = df_result["latitude"].astype(float)
        df_result["longitude"] = df_result["longitude"].astype(float)
        
        data_df.set(df_result)
        status_message.set(f"{code} 已載入 {len(df_result)} 筆城市資料")
                
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())


# ----------------------------------------------------
# 3. 視覺化組件 (整合修正後的 Leafmap 核心邏輯)
# ----------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):
    
    # 創建地圖實例 (確保只執行一次)
    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2,
            center=[0, 0], 
            add_sidebar=True,
            sidebar_visible=True,
        ),
        []
    )
    
    # 在 use_memo 後設定地圖樣式和控制項
    if not hasattr(m, '_initialized_base_layers'):
        m.add_basemap("Esri.WorldImagery")
        m.add_draw_control(controls=["polygon", "trash"])
        m.layout.height = "900px"  # 在 Map 實例上設定高度
        m._initialized_base_layers = True


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

            # 確保 GeoJSON 數據類型正確
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]}, # [lon, lat] 順序
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

        # ⭐ 修正: 使用 fit_bounds 實現自動縮放
        if len(lats) > 0:
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)
            m.fit_bounds([[min_lon, min_lat], [max_lon, max_lat]])


    # 監聽 df 內容的變化
    solara.use_effect(update_layer, [df.values.tolist()]) 
    
    return m.to_solara()


# ----------------------------------------------------
# 4. 頁面佈局組件
# ----------------------------------------------------
@solara.component
def Page():
    # 執行數據載入邏輯
    solara.use_effect(load_country_list, [])
    solara.use_effect(load_filtered_data, [selected_country.value])

    # 檢查是否在載入中 (避免在列表為空時渲染 Select)
    if not all_countries.value and status_message.value != "國家列表載入完成":
         return solara.Info("正在載入國家清單...")

    # 數據表 (僅在數據非空時顯示)
    city_table = None
    if not data_df.value.empty:
        # 選擇需要的欄位並重新命名
        df_for_table = data_df.value[['name', 'country', 'latitude', 'longitude', 'population']].rename(
            columns={'name': '城市名稱', 'country': '代碼', 'latitude': '緯度', 'longitude': '經度', 'population': '人口'}
        )
        city_table = solara.Column(
            children=[
                solara.Markdown("### 城市清單與座標詳情"),
                solara.DataTable(df_for_table)
            ]
        )
    
    # 組合頁面佈局
    return solara.Column([
        
        solara.Card(title="城市數據篩選與狀態", elevation=2),

        # 1. 控制項和狀態
        solara.Select(
            label="選擇國家代碼",
            value=selected_country,
            values=all_countries.value
        ),
        solara.Markdown(f"**狀態：** {status_message.value}"),

        solara.Markdown("---"),
        
        # 2. 地圖
        CityMap(data_df.value),
        
        # 3. 表格
        city_table,
    ])