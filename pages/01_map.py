import solara
import duckdb
import pandas as pd
import leafmap.maplibregl as leafmap
import numpy as np

# ----------------------------------------------------
# 0. 常量
# ----------------------------------------------------
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

# ----------------------------------------------------
# 1. 狀態管理 (Reactive Variables)
# ----------------------------------------------------
all_countries = solara.reactive([])
selected_country = solara.reactive("TWN")
data_df = solara.reactive(pd.DataFrame())
status_message = solara.reactive("初始化中...")

# ----------------------------------------------------
# 2. 數據獲取邏輯 (不變)
# ----------------------------------------------------

def load_country_list():
    status_message.set("正在載入國家列表...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        
        rows = con.sql(f"""
            SELECT DISTINCT country 
            FROM '{CITIES_CSV_URL}'
            ORDER BY country;
        """).fetchall()
        
        all_countries.set([r[0] for r in rows])
        
        if not all_countries.value:
             status_message.set("警告：國家列表為空。")
             
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：無法載入國家列表 {e}")

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
        
        df_result["latitude"] = df_result["latitude"].astype(float)
        df_result["longitude"] = df_result["longitude"].astype(float)
        
        data_df.set(df_result)
        status_message.set(f"{code} 已載入 {len(df_result)} 筆城市資料")
                
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())

# -----------------------------------------------------------
## 🗺️ 地圖元件
# -----------------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2,
            center=[0, 0], 
            add_sidebar=True,
            sidebar_visible=True,
        ),
        []
    )
    
    # 設置底圖和控制項
    if not hasattr(m, '_initialized_base_layers'):
        m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id, visible=False)
        m.add_draw_control(controls=["polygon", "trash"])
        m.layout.height = "900px" 
        m._initialized_base_layers = True


    def update_layer():
        LAYER = "city_points"
        
        # 移除舊圖層 (僅移除 Layer，不移除 Source)
        try:
            m.remove_layer(LAYER)
        except Exception:
            pass

        if df.empty:
            return

        features = []
        lats, lons = [], []
        
        # 轉換 GeoJSON
        for index, row in df.iterrows():
            try:
                lon, lat = float(row["longitude"]), float(row["latitude"])
                population = int(row["population"]) if pd.notna(row.get("population")) else None
            except Exception:
                continue 
            
            lats.append(lat)
            lons.append(lon)

            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "name": row["name"],
                    "country": row["country"],
                    "population": population
                }
            })

        geojson = {"type": "FeatureCollection", "features": features}

        if not features:
            status_message.set(f"警告：所有城市數據轉換失敗，未繪製點位 (代碼: {selected_country.value})。")
            return
            
        # ⭐ 關鍵修正：替換為單一 m.add_geojson 呼叫
        # 嘗試使用 layer_name 來確保 remove_layer 能夠工作
        m.add_geojson(
            geojson
        )

        # 自動 zoom to bounds
        if len(lats) > 0:
            min_lat, max_lat = min(lats), max(lats)
            min_lon, max_lon = min(lons), max(lons)
            m.fit_bounds([[min_lon, min_lat], [max_lon, max_lat]])
            
        status_message.set(f"成功：已找到 {len(features)} 個城市點位！ (代碼: {selected_country.value})")

    solara.use_effect(update_layer, [df]) 
    return m.to_solara()


# -----------------------------------------------------------
## 📑 Solara Page() 
# -----------------------------------------------------------
@solara.component
def Page():

    # 初始化
    solara.use_effect(load_country_list, [])
    solara.use_effect(load_filtered_data, [selected_country.value])

    # 檢查是否在載入中
    if not all_countries.value and status_message.value != "國家列表載入完成":
         return solara.Info("正在載入國家清單...")

    # 城市表格 (僅在數據非空時顯示)
    city_table = None
    df = data_df.value
    if not df.empty:
        df_for_table = df[['name', 'country', 'latitude', 'longitude', 'population']].rename(
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
         # 2. 表格
        city_table,
        # 3. 地圖
        CityMap(data_df.value),
        
    ])
