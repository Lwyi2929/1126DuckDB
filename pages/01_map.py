import solara
import duckdb
import pandas as pd
import leafmap.maplibregl as leafmap
import numpy as np

CITIES_CSV_URL = 'https://data.gishub.org/duckdb/cities.csv'
MIN_POP_DEFAULT = 100000
MAX_POP_DEFAULT = 5000000
MAX_POP_SLIDER = 15000000

data_df = solara.reactive(pd.DataFrame())
status_message = solara.reactive("初始化中...")
min_pop_value = solara.reactive(MIN_POP_DEFAULT)
max_pop_value = solara.reactive(MAX_POP_DEFAULT)
country_pop_bounds = solara.reactive((0, MAX_POP_SLIDER))


# ----------------------------------------------------
# 1. 全域人口邊界
# ----------------------------------------------------
def load_global_pop_bounds():
    status_message.set("正在載入全域人口邊界...")
    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        result = con.sql(f"SELECT MIN(population), MAX(population) FROM '{CITIES_CSV_URL}';").fetchone()
        con.close()

        min_pop_actual = int(result[0])
        max_pop_actual = int(result[1])

        max_pop_rounded = int(np.ceil(max_pop_actual / 100000.0)) * 100000
        if max_pop_rounded < 100000:
            max_pop_rounded = 100000

        country_pop_bounds.set((min_pop_actual, max_pop_rounded))

        min_pop_value.set(min_pop_actual)
        max_pop_value.set(max_pop_rounded)

        status_message.set(f"全域人口範圍載入完成: {min_pop_actual:,} - {max_pop_rounded:,}")

    except Exception as e:
        status_message.set(f"錯誤：載入人口邊界失敗 {e}")
        country_pop_bounds.set((0, MAX_POP_SLIDER))


# ----------------------------------------------------
# 2. 載入篩選後的城市資料
# ----------------------------------------------------
def load_filtered_data():
    min_pop = min_pop_value.value
    max_pop = max_pop_value.value

    if min_pop > max_pop:
        status_message.set("錯誤：最低人口不能大於最高人口。")
        data_df.set(pd.DataFrame())
        return

    status_message.set(f"載入全球城市中 (人口 {min_pop:,} ~ {max_pop:,}) ...")

    try:
        con = duckdb.connect()
        con.install_extension("httpfs"); con.load_extension("httpfs")
        sql_query = f"""
        SELECT name, country, population, latitude, longitude
        FROM '{CITIES_CSV_URL}'
        WHERE population BETWEEN {min_pop} AND {max_pop}
        ORDER BY population DESC
        LIMIT 200;
        """
        df_result = con.sql(sql_query).df()
        con.close()

        df_result["latitude"] = df_result["latitude"].astype(float)
        df_result["longitude"] = df_result["longitude"].astype(float)

        data_df.set(df_result)
        status_message.set(f"成功：載入 {len(df_result)} 筆城市資料")

    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 {e}")
        data_df.set(pd.DataFrame())


# ----------------------------------------------------
# 3. CityMap 元件
# ----------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):
    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2,
            center=[0, 0],
            height="800px",
            width="100%",
            add_sidebar=True,
            sidebar_width="300px",
        ),
        []
    )

    if not hasattr(m, "_initialized"):
        m.add_basemap("OpenTopoMap")
        m._initialized = True

    def update_layer():
        try:
            m.remove_layer("city_points")
            m.remove_source("city_source")
        except Exception:
            pass

        if df.empty:
            return

        features = []
        lats, lons = [], []

        for _, row in df.iterrows():
            lon, lat = float(row["longitude"]), float(row["latitude"])
            lats.append(lat)
            lons.append(lon)
            features.append({
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "name": row["name"],
                    "population": row["population"]
                }
            })

        geojson = {"type": "FeatureCollection", "features": features}

        m.add_source("city_source", geojson)
        m.add_layer({
            "id": "city_points",
            "type": "circle",
            "source": "city_source",
            "paint": {
                "circle-radius": 6,
                "circle-color": "red",
            }
        })

        if lats:
            m.fit_bounds([
                [min(lons), min(lats)],
                [max(lons), max(lats)]
            ])

    solara.use_effect(update_layer, [df])
    return m.to_solara()


# ----------------------------------------------------
# 4. 主頁面
# ----------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_global_pop_bounds, [])
    solara.use_effect(load_filtered_data, [min_pop_value.value, max_pop_value.value])

    min_bound, max_bound = country_pop_bounds.value

    df = data_df.value

    # city_table 不允許為 None
    if df.empty:
        city_table = solara.Markdown("尚無城市資料。請調整人口篩選條件。")
    else:
        df_show = df.rename(columns={
            "name": "城市名稱",
            "country": "國家",
            "latitude": "緯度",
            "longitude": "經度",
            "population": "人口"
        })
        city_table = solara.DataTable(df_show)

    return solara.Column(
        gap="20px",
        children=[
            solara.Card("城市資料篩選", elevation=2),

            solara.SliderInt(
                label=f"最低人口：{min_pop_value.value:,}",
                value=min_pop_value,
                min=min_bound,
                max=max_bound,
                step=50000
            ),
            solara.SliderInt(
                label=f"最高人口：{max_pop_value.value:,}",
                value=max_pop_value,
                min=min_bound,
                max=max_bound,
                step=50000
            ),

            solara.Markdown(f"**狀態：** {status_message.value}"),
            solara.Markdown("---"),

            CityMap(df),
            solara.Markdown("### 城市列表（含經緯度）"),
            city_table
        ]
    )
