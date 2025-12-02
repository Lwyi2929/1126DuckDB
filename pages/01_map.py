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
data_df = solara.reactive(pd.DataFrame()) 
status_message = solara.reactive("初始化中...")
min_pop_value = solara.reactive(MIN_POP_DEFAULT) 
max_pop_value = solara.reactive(MAX_POP_DEFAULT) 
country_pop_bounds = solara.reactive((0, MAX_POP_SLIDER)) 


# ----------------------------------------------------
# 2. 數據獲取邏輯
# ----------------------------------------------------

def load_global_pop_bounds():
    status_message.set("正在載入全域人口邊界...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        result = con.sql(f"SELECT MIN(population), MAX(population) FROM '{CITIES_CSV_URL}';").fetchone()
        con.close()

        min_pop_actual = int(result[0]) if result[0] is not None else 0
        max_pop_actual = int(result[1]) if result[1] is not None else MAX_POP_SLIDER
        
        max_pop_rounded = int(np.ceil(max_pop_actual / 100000.0)) * 100000
        if max_pop_rounded < 100000: max_pop_rounded = 100000
        
        country_pop_bounds.set((min_pop_actual, max_pop_rounded))
        
        min_pop_value.set(min_pop_actual)
        max_pop_value.set(max_pop_rounded)
        status_message.set(f"全域人口範圍載入完成: {min_pop_actual:,} - {max_pop_rounded:,}")

    except Exception as e:
        status_message.set(f"錯誤：載入全域人口邊界失敗 ({e})")
        country_pop_bounds.set((0, MAX_POP_SLIDER))

def load_filtered_data():
    min_pop = min_pop_value.value
    max_pop = max_pop_value.value
    
    if min_pop > max_pop:
        status_message.set("錯誤：最低人口不能大於最高人口。")
        data_df.set(pd.DataFrame()); return
        
    status_message.set(f"正在查詢全球城市 (人口 {min_pop:,} - {max_pop:,})...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        
        # 為了避免在地圖上產生過多點位造成性能問題，我們繼續限制查詢結果
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE population BETWEEN {min_pop} AND {max_pop} 
        ORDER BY population DESC
        LIMIT 1000;
        """
        df_result = con.sql(sql_query).df()
        
        df_result["latitude"] = df_result["latitude"].astype(float)
        df_result["longitude"] = df_result["longitude"].astype(float)
        
        data_df.set(df_result)
        status_message.set(f"成功：載入 {len(df_result)} 筆全球城市資料")
                
        con.close()
    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())

# -----------------------------------------------------------
# 3. 視覺化組件 (CityMap) - 實現集群
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
        m.layout.width = "100%"; m.layout.height = "900px";
        m._initialized_base_layers = True

    def update_layer():
        # 定義不同的圖層ID
        SOURCE = "city_source"
        CLUSTER_LAYER = "clusters"
        CLUSTER_COUNT_LAYER = "cluster-count"
        UNCLUSTERED_LAYER = "unclustered-point"
        
        # 移除舊圖層和數據源
        for layer in [CLUSTER_LAYER, CLUSTER_COUNT_LAYER, UNCLUSTERED_LAYER]:
            try: m.remove_layer(layer)
            except Exception: pass
        try: m.remove_source(SOURCE)
        except Exception: pass
        
        if df.empty: return

        features = []; lats, lons = []
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
            status_message.set(f"警告：城市數據轉換失敗，未繪製點位。")
            return
            
        # ⭐ 步驟 1: 添加數據源並啟用集群 (cluster=True)
        m.add_source(SOURCE, geojson, cluster=True, clusterMaxZoom=14, clusterRadius=50)

        # ⭐ 步驟 2: 繪製集群圓圈 (Clusters circles)
        m.add_layer({
            "id": CLUSTER_LAYER,
            "type": "circle",
            "source": SOURCE,
            "filter": ["has", "point_count"], # 僅顯示具有計數屬性的點 (即集群)
            "paint": {
                # 根據集群中的點位數量改變顏色
                "circle-color": [
                    "step", ["get", "point_count"],
                    "#51bbd6", 100, "#f1f075", 750, "#f28cb1"
                ],
                "circle-radius": [
                    "step", ["get", "point_count"],
                    20, 100, 30, 750, 40
                ]
            }
        })

        # ⭐ 步驟 3: 繪製集群計數 (Cluster count)
        m.add_layer({
            "id": CLUSTER_COUNT_LAYER,
            "type": "symbol",
            "source": SOURCE,
            "filter": ["has", "point_count"],
            "layout": {
                "text-field": ["get", "point_count_abbreviated"],
                "text-font": ["DIN Offc Pro Medium", "Arial Unicode MS Bold"],
                "text-size": 12
            },
            "paint": {
                "text-color": "#ffffff"
            }
        })

        # ⭐ 步驟 4: 繪製非集群點位 (Unclustered points)
        m.add_layer({
            "id": UNCLUSTERED_LAYER,
            "type": "circle",
            "source": SOURCE,
            "filter": ["!", ["has", "point_count"]], # 僅顯示沒有計數屬性的點 (即單獨點位)
            "paint": {
                "circle-color": "#11b4da",
                "circle-radius": 4,
                "circle-stroke-width": 1,
                "circle-stroke-color": "#fff"
            }
        })

        # 調整地圖視圖
        if len(lats) > 0:
            min_lat, max_lat = min(lats), max(lats); min_lon, max_lon = min(lons), max(lons)
            m.fit_bounds([[min_lon, min_lat], [max_lon, max_lat]])
            
        status_message.set(f"成功：已找到 {len(features)} 個城市點位，並啟用集群顯示！")

    solara.use_effect(update_layer, [df]) 
    return m.to_solara()

# -----------------------------------------------------------
# 4. 主頁面組件 (Page)
# -----------------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_global_pop_bounds, []) 
    solara.use_effect(load_filtered_data, [min_pop_value.value, max_pop_value.value])

    min_available_pop, max_available_pop = country_pop_bounds.value
    
    if max_available_pop == MAX_POP_SLIDER and status_message.value.startswith("正在載入"):
         return solara.Info("正在載入全域人口邊界...")

    # 城市表格 (已修正限制邏輯)
    city_table = None
    df = data_df.value
    
    if not df.empty:
        num_records = len(df)
        warning_message = None
        
        # 邏輯：限制顯示筆數
        if num_records > 300:
            # 這裡我們使用 head(300) 來確保表格能夠展示最多的數據，因為數據查詢上限是 1000
            df_to_display = df.head(300) 
            warning_message = solara.Warning(f"資料量超過 300 筆 ({num_records} 筆)。表格僅顯示前 300 筆記錄。", dense=True)
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
        
        final_children = [child for child in children if child is not None]
        city_table = solara.Column(final_children)
    
    # 組合頁面佈局 (已修復 TraitError 的 None 過濾問題)
    main_components = [
        solara.Card(title="城市數據篩選與狀態", elevation=2),

        # 1. 控制項和狀態 (已修復 TraitError 的 label 格式化問題)
        solara.SliderInt(
            label="最低人口 (人): {:,.0f}".format(min_pop_value.value),
            value=min_pop_value,
            min=min_available_pop,
            max=max_available_pop,
            step=50000
        ),
        solara.SliderInt(
            label="最高人口 (人): {:,.0f}".format(max_pop_value.value),
            value=max_pop_value,
            min=min_available_pop,
            max=max_available_pop,
            step=50000
        ),
        
        solara.Markdown(f"**狀態：** {status_message.value}"),
        solara.Markdown("---"),
        
        # 2. 地圖
        CityMap(data_df.value),
        
        # 3. 表格 (可能為 None)
        city_table,
    ]
    
    # 使用列表推導式過濾所有 None 值，確保佈局穩定
    return solara.Column([item for item in main_components if item is not None])


