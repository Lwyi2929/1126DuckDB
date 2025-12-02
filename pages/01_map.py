import solara
import duckdb
import pandas as pd
import plotly.express as px # 雖然程式中沒有使用，但保留
import leafmap.maplibregl as leafmap

# -----------------------------
# 1. 全域狀態管理
# -----------------------------
CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'

all_countries = solara.reactive([])
selected_country = solara.reactive("")
population_threshold = solara.reactive(1_000_000)   # 人口門檻

data_df = solara.reactive(pd.DataFrame())

# -----------------------------
# 2. 載入國家清單
# -----------------------------
def load_country_list():
    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        result = con.sql(f"""
            SELECT DISTINCT country
            FROM '{CITIES_CSV_URL}'
            ORDER BY country
        """).fetchall()

        country_list = [row[0] for row in result]
        all_countries.set(country_list)

        # 預設選 USA 或第一個
        if "USA" in country_list:
            selected_country.set("USA")
        elif country_list:
            selected_country.set(country_list[0])

        con.close()
    except Exception as e:
        print("Error loading countries:", e)

# -----------------------------
# 3. 載入該國家 + 人口門檻的城市
# -----------------------------
def load_filtered_data():
    country_name = selected_country.value
    threshold = population_threshold.value

    if not country_name:
        return

    try:
        con = duckdb.connect()
        con.install_extension("httpfs")
        con.load_extension("httpfs")

        df_result = con.sql(f"""
            SELECT name, country, population, latitude, longitude
            FROM '{CITIES_CSV_URL}'
            WHERE country = '{country_name}'
              AND population >= {threshold}
            ORDER BY population DESC
            LIMIT 200; # 增加限制，以確保 GeoJSON 數據合理
        """).df()

        data_df.set(df_result)
        con.close()

    except Exception as e:
        print("Error loading filtered cities:", e)
        data_df.set(pd.DataFrame())

# -----------------------------
# 4. Leafmap 地圖元件
# -----------------------------
@solara.component
def CityMap(df: pd.DataFrame):
    if df.empty:
        # 顯示警告訊息，而不是 Info
        return solara.Warning("沒有城市數據符合當前人口門檻。") 

    # 地圖中心點設為人口最大的城市
    # 確保座標轉換為 float 類型
    lon = df['longitude'].iloc[0].astype(float)
    lat = df['latitude'].iloc[0].astype(float)
    center = [lat, lon]

    # 使用 use_memo 確保地圖只初始化一次
    m = solara.use_memo(
        lambda: leafmap.Map(
            center=center,
            zoom=4,
            add_sidebar=True,
            height="600px"
        ), []
    )
    
    # 設置底圖和控制項
    m.add_basemap("Esri.WorldImagery", before_id=m.first_symbol_layer_id)

    # 轉成 GeoJSON
    features = []
    for _, row in df.iterrows():
        # 顯式轉換數據類型
        population = int(row["population"]) if pd.notna(row["population"]) else None
        
        features.append({
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [float(row["longitude"]), float(row["latitude"])]
            },
            "properties": {
                "name": row["name"],
                "country": row["country"],
                "population": population
            }
        })

    geojson = {"type": "FeatureCollection", "features": features}
    
    # 清除舊圖層 (由於 add_geojson 在 Solara 中會導致圖層疊加)
    try:
        # 假設圖層名稱是固定的
        m.remove_layer("geojson_layer_0") 
    except Exception:
        pass
        
    m.add_geojson(geojson)

    return m.to_solara()

# -----------------------------
# 5. Solara 主頁面
# -----------------------------
@solara.component
def Page():

    solara.use_effect(load_country_list, dependencies=[])

    solara.use_effect(
        load_filtered_data,
        dependencies=[selected_country.value, population_threshold.value]
    )

    with solara.Card(title="城市篩選器"):
        solara.Select(
            label="選擇國家",
            value=selected_country,
            values=all_countries.value
        )

        # ⭐ 人口門檻 slider
        solara.SliderInt(
            label="人口下限",
            value=population_threshold,
            min=0,
            max=20_000_000,
            step=100_000
        )
        solara.Markdown(f"目前人口門檻：**{population_threshold.value:,}**")

    df = data_df.value

    # 主內容顯示區塊 (只有在有選定國家且有數據時才顯示地圖和表格)
    if selected_country.value and not df.empty:
        
        # 標題
        solara.Markdown(f"## {selected_country.value}（人口 ≥ {population_threshold.value:,}）")
        
        
        
        # 表格
        solara.Markdown("###表格")
        solara.DataFrame(df)
        # 地圖元件
        CityMap(df) 
    elif selected_country.value: 
        # 處理沒有數據但國家已選的情況
        solara.Info(f"{selected_country.value} 沒有城市符合當前人口門檻：{population_threshold.value:,}")
    else:
        # 處理國家清單尚未載入的情況
        solara.Info("正在載入國家清單...")