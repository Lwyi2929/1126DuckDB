import solara
import duckdb
import pandas as pd
import leafmap.maplibregl as leafmap
import numpy as np

# ----------------------------------------------------
# 0. 常量與預設值
# ----------------------------------------------------
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'
MIN_POP_DEFAULT = 100000
MAX_POP_DEFAULT = 5000000
MAX_POP_SLIDER = 15000000 

# ----------------------------------------------------
# 1. 狀態管理 (Reactive Variables)
# ----------------------------------------------------
all_countries = solara.reactive([])
selected_country = solara.reactive("TWN")
data_df = solara.reactive(pd.DataFrame())
status_message = solara.reactive("初始化中...")

# ⭐ 修正：使用兩個單獨的響應式變數 (最穩定的 Slider 解決方案)
min_pop_value = solara.reactive(MIN_POP_DEFAULT) 
max_pop_value = solara.reactive(MAX_POP_DEFAULT) 
country_pop_bounds = solara.reactive((0, MAX_POP_SLIDER)) 


# ----------------------------------------------------
# 2. 數據獲取邏輯
# ----------------------------------------------------

def load_country_list():
    status_message.set("正在載入國家列表...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        result = con.sql(f"SELECT DISTINCT country FROM '{CITIES_CSV_URL}' ORDER BY country;").fetchall()
        all_countries.set([r[0] for r in result])
        if not all_countries.value: status_message.set("警告：國家列表為空。")
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入國家列表失敗 ({e})")

# B. 載入選定國家的人口邊界 (MIN/MAX)
def load_country_pop_bounds():
    code = selected_country.value
    if not code:
        country_pop_bounds.set((0, MAX_POP_SLIDER)); return

    try:
        con = duckdb.connect()
        # 查詢當前國家的實際人口範圍
        result = con.sql(f"SELECT MIN(population), MAX(population) FROM '{CITIES_CSV_URL}' WHERE country = '{code}';").fetchone()
        con.close()

        min_pop_actual = int(result[0]) if result[0] is not None else 0
        max_pop_actual = int(result[1]) if result[1] is not None else MAX_POP_SLIDER
        
        # 將最大值向上取整到最近的 10萬
        max_pop_rounded = int(np.ceil(max_pop_actual / 100000.0)) * 100000
        if max_pop_rounded < 100000: max_pop_rounded = 100000
        
        country_pop_bounds.set((min_pop_actual, max_pop_rounded))
        
        # 關鍵：重設 min/max_pop_value，避免超出新邊界
        if min_pop_value.value < min_pop_actual: min_pop_value.set(min_pop_actual)
        if max_pop_value.value > max_pop_rounded: max_pop_value.set(max_pop_rounded)
        
    except Exception as e:
        print(f"Error loading bounds: {e}")
        country_pop_bounds.set((0, MAX_POP_SLIDER))

# C. 根據選中的國家篩選城市數據
def load_filtered_data():
    code = selected_country.value
    # ⭐ 從單獨變數獲取 MIN/MAX 值
    min_pop = min_pop_value.value
    max_pop = max_pop_value.value
    
    # 安全檢查
    if min_pop > max_pop:
        status_message.set("錯誤：最低人口不能大於最高人口。")
        data_df.set(pd.DataFrame())
        return

    if not code: data_df.set(pd.DataFrame()); return 
        
    status_message.set(f"正在查詢 {code} (人口 {min_pop:,} - {max_pop:,})...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE country = '{code}'
          AND population BETWEEN {min_pop} AND {max_pop} 
        ORDER BY population DESC
        LIMIT 200;
        """
        df_result = con.sql(sql_query).df()
        
        df_result["latitude"] = df_result["latitude"].astype(float)
        df_result["longitude"] = df_result["longitude"].astype(float)
        
        data_df.set(df_result)
        status_message.set(f"成功：載入 {code} 的 {len(df_result)} 筆城市資料")
                
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())

# -----------------------------------------------------------
# 3. 視覺化組件 (保持穩定)
# -----------------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    m = solara.use_memo(
        lambda: leafmap.Map(zoom=2, center=[0, 0], add_sidebar=True, sidebar_visible=True,),
        []
    )
    if not hasattr(m, '_initialized_base_layers'):
        m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id, visible=False)
        m.add_draw_control(controls=["polygon", "trash"])
        m.layout.height = "900px"; m._initialized_base_layers = True

    def update_layer():
        LAYER = "city_points"; SOURCE = "city_source"
        try: m.remove_layer(LAYER); m.remove_source(SOURCE)
        except Exception: pass
        if df.empty: return

        features = []; lats, lons = [], []
        for index, row in df.iterrows():
            try: lon, lat = float(row["longitude"]), float(row["latitude"])
            except Exception: continue
            lats.append(lat); lons.append(lon)
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {"name": row["name"], "population": row["population"]},
            })
        geojson = {"type": "FeatureCollection", "features": features}

        if not features:
            status_message.set(f"警告：所有城市數據轉換失敗，未繪製點位 (代碼: {selected_country.value})。")
            return
            
        m.add_source(SOURCE, geojson)
        m.add_layer({"id": LAYER, "type": "circle", "source": SOURCE,
            "paint": {"circle-radius": 6, "circle-color": "red", "circle-opacity": 0.9,},
        })

        if len(lats) > 0:
            min_lat, max_lat = min(lats), max(lats); min_lon, max_lon = min(lons), max(lons)
            m.fit_bounds([[min_lon, min_lat], [max_lon, max_lat]])
            
        status_message.set(f"成功：已找到 {len(features)} 個城市點位！ (代碼: {selected_country.value})")

    solara.use_effect(update_layer, [df]) 
    return m.to_solara()

# -----------------------------------------------------------
# 4. 主頁面組件
# -----------------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_country_list, [])
    solara.use_effect(load_country_pop_bounds, [selected_country.value]) 
    # ⭐ 監聽國家和兩個滑塊的變化
    solara.use_effect(load_filtered_data, [selected_country.value, min_pop_value.value, max_pop_value.value])

    if not all_countries.value and status_message.value != "國家列表載入完成":
         return solara.Info("正在載入國家清單...")

    min_available_pop, max_available_pop = country_pop_bounds.value
    
    city_table = None; df = data_df.value
    if not df.empty:
        df_for_table = df[['name', 'country', 'latitude', 'longitude', 'population']].rename(
            columns={'name': '城市名稱', 'country': '代碼', 'latitude': '緯度', 'longitude': '經度', 'population': '人口'}
        )
        city_table = solara.Column([solara.Markdown("### 城市清單與座標詳情"), solara.DataTable(df_for_table)])
    
    return solara.Column([

        solara.Card(title="城市數據篩選與狀態", elevation=2),

        solara.Select(label="選擇國家代碼", value=selected_country, values=all_countries.value),
        
        # ⭐ 修正點：使用兩個 solara.SliderInt
        solara.SliderInt(
            label=f"最低人口 (人): {min_pop_value.value:,}",
            value=min_pop_value,
            min=min_available_pop,
            max=max_available_pop,
            step=50000
        ),
        solara.SliderInt(
            label=f"最高人口 (人): {max_pop_value.value:,}",
            value=max_pop_value,
            min=min_available_pop,
            max=max_available_pop,
            step=50000
        ),
        
        solara.Markdown(f"**狀態：** {status_message.value}"),
        solara.Markdown("---"),
        
        CityMap(data_df.value),
        city_table,
    ])