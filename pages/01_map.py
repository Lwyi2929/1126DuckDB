import json
import duckdb
import solara
import ipywidgets as widgets
import leafmap.maplibregl as leafmap
import matplotlib.pyplot as plt


def create_map():

    m = leafmap.Map(
        add_sidebar=True,
        add_floating_sidebar=False,
        sidebar_visible=True,
        layer_manager_expanded=False,
        height="800px",
    )
    m.add_basemap("Esri.WorldImagery")


    con = duckdb.connect()
    con.install_extension("httpfs")
    con.install_extension("spatial")
    con.install_extension("h3", repository="community")
    con.load_extension("httpfs")
    con.load_extension("spatial")
    
# 1. 定義資料 URL
    city_url = "https://data.gishub.org/duckdb/cities.csv"
    country_list_url = 'https://data.gishub.org/duckdb/countries.csv'


# 2. 創建城市資料表 (修正後的 SQL，移除 print/try/except 簡化)
    con.sql(f"""
    CREATE OR REPLACE TABLE city_geom AS
    SELECT
        *,
        ST_Point(longitude, latitude) AS geom
    FROM read_csv_auto('{city_url}');
""")


# 3. 獲取不重複的國家列表 (移除 Pandas 依賴，使用 DuckDB 原生方法)
    country_query_result = con.sql(f"""
    SELECT DISTINCT country
    FROM read_csv_auto('{country_list_url}')
    ORDER BY country;
""")

# 將 DuckDB 結果 (tuple 列表) 轉換為單純的國家名稱列表
    country_list = [row[0] for row in country_query_result.fetchall()]


# 4. 初始化響應式變數
# 設置初始值為列表中的第一個國家，若列表為空則預設為 "Taiwan"
    initial_country = country_list[0] if country_list else "Taiwan"
    country
    country=solara.reactive(initial_country)


# 5. Solara 組件
@solara.component
def Page():
    solara.Select(label="Country", value=country, values=country_list)
    solara.Markdown(f"**Selected**: {country.value}")