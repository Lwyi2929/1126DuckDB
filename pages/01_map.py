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
# 1. 狀態管理
# ----------------------------------------------------
data_df = solara.reactive(pd.DataFrame())
status_message = solara.reactive("初始化中...")
min_pop_value = solara.reactive(MIN_POP_DEFAULT)
max_pop_value = solara.reactive(MAX_POP_DEFAULT)
country_pop_bounds = solara.reactive((0, MAX_POP_SLIDER))


# ----------------------------------------------------
# 2. 數據讀取
# ----------------------------------------------------
def load_global_pop_bounds():
    status_message.set("正在載入全域人口邊界...")
    try:
        with duckdb.connect() as con:
            con.install_extension("httpfs")
            con.load_extension("httpfs")
            result = con.sql(
                f"SELECT MIN(population), MAX(population) FROM '{CITIES_CSV_URL}';"
            ).fetchone()

        min_pop_actual = int(result[0] or 0)
        max_pop_actual = int(result[1] or MAX_POP_SLIDER)

        # 取比較漂亮的範圍
        max_pop_rounded = max(100000, int(np.ceil(max_pop_actual / 100000.0)) * 100000)

        country_pop_bounds.set((min_pop_actual, max_pop_rounded))

        # update slider initial values
        min_pop_value.set(min_pop_actual)
        max_pop_value.set(max_pop_rounded)

        status_message.set(
            f"全域人口範圍載入完成: {min_pop_actual:,} - {max_pop_rounded:,}"
        )

    except Exception as e:
        status_message.set(f"錯誤：載入全域人口邊界失敗 ({e})")
        country_pop_bounds.set((0, MAX_POP_SLIDER))
        data_df.set(pd.DataFrame())


def load_filtered_data():
    min_pop = min_pop_value.value
    max_pop = max_pop_value.value

    if min_pop > max_pop:
        status_message.set("錯誤：最低人口不能大於最高人口。")
        data_df.set(pd.DataFrame())
        return

    status_message.set(f"正在查詢城市：{min_pop:,} - {max_pop:,}")

    try:
        with duckdb.connect() as con:
            con.install_extension("httpfs")
            con.load_extension("httpfs")

            sql_query = f"""
            SELECT name, country, population, latitude, longitude
            FROM '{CITIES_CSV_URL}'
            WHERE population BETWEEN {min_pop} AND {max_pop}
            ORDER BY population DESC
            LIMIT 200;
            """
            df = con.sql(sql_query).df()

        df["latitude"] = df["latitude"].astype(float)
        df["longitude"] = df["longitude"].astype(float)

        data_df.set(df)
        status_message.set(f"成功：載入 {len(df)} 筆城市資料")

    except Exception as e:
        status_message.set(f"錯誤：載入城市資料失敗 ({e})")
        data_df.set(pd.DataFrame())


# ----------------------------------------------------
# 3. 地圖組件
# ----------------------------------------------------
@solara.component
def CityMap(df: pd.DataFrame):

    # map instance 保留
    m = solara.use_memo(
        lambda: leafmap.Map(
            zoom=2, center=[0, 0], add_sidebar=True, sidebar_visible=True
        ),
        [],
    )

    # 初始化地圖（只執行一次）
    if not hasattr(m, "_initialized"):
        m.add_basemap("Esri.WorldImagery", visible=False)
        m.add_draw_control(controls=["polygon", "trash"])
        m.layout.width = "100%"
        m.layout.height = "900px"
        m._initialized = True

    def update_layer():

        layer_id = "city_points"
        source_id = "city_source"

        # 清掉之前的 layer
        try:
            m.remove_layer(layer_id)
            m.remove_source(source_id)
        except Exception:
            pass

        if df.empty:
            return

        # Convert df to GeoJSON
        features = []
        lats, lons = [], []

        for _, row in df.iterrows():
            lat = float(row["latitude"])
            lon = float(row["longitude"])
            lats.append(lat)
            lons.append(lon)
            features.append(
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": {
                        "name": row["name"],
                        "population": row["population"],
                    },
                }
            )

        geojson = {"type": "FeatureCollection", "features": features}

        m.add_source(source_id, geojson)
        m.add_layer(
            {
                "id": layer_id,
                "type": "circle",
                "source": source_id,
                "paint": {
                    "circle-radius": 6,
                    "circle-color": "red",
                    "circle-opacity": 0.9,
                },
            }
        )

        # zoom to bounds
        m.fit_bounds(
            [[min(lons), min(lats)], [max(lons), max(lats)]]
        )

        status_message.set(f"成功：已找到 {len(features)} 個城市點位！")

    solara.use_effect(update_layer, [df])
    return m.to_solara()


# ----------------------------------------------------
# 4. 主頁面
# ----------------------------------------------------
@solara.component
def Page():

    solara.use_effect(load_global_pop_bounds, [])
    solara.use_effect(
        load_filtered_data, [min_pop_value.value, max_pop_value.value]
    )

    min_avail, max_avail = country_pop_bounds.value

    # 還沒載入完成就避免渲染錯誤
    if status_message.value.startswith("正在載入"):
        return solara.Info("正在載入資料...")

    df = data_df.value

    table_block = None
    if not df.empty:
        df_show = df.rename(
            columns={
                "name": "城市名稱",
                "country": "代碼",
                "latitude": "緯度",
                "longitude": "經度",
                "population": "人口",
            }
        )
        table_block = solara.Column(
            [
                solara.Markdown("### 城市清單與座標"),
                solara.DataTable(df_show),
            ]
        )

    return solara.Column(
        [
            solara.Card(
                solara.Column(
                    [
                        solara.SliderInt(
                            label=f"最低人口: {min_pop_value.value:,}",
                            value=min_pop_value,
                            min=min_avail,
                            max=max_avail,
                            step=50000,
                        ),
                        solara.SliderInt(
                            label=f"最高人口: {max_pop_value.value:,}",
                            value=max_pop_value,
                            min=min_avail,
                            max=max_avail,
                            step=50000,
                        ),
                        solara.Info(status_message.value),
                        solara.Markdown("---"),
                    ]
                ),
                title="城市資料篩選控制",
                elevation=2,
            ),
            CityMap(df),
            table_block,
        ]
    )
