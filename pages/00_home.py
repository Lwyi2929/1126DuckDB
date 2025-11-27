import solara
import leafmap.leafmap as leafmap # 使用 ipyleaflet (重量級) 後端


@solara.component
def Page():
    with solara.Column(align="center"):
        markdown = """
        ## DuckDB 空間資料庫與地理資訊系統應用示範
        本範例展示如何使用 DuckDB 空間資料庫進行地理資訊系統 (GIS) 的應用。DuckDB 提供了強大的 SQL 查詢能力，並且支援空間資料處理，讓我們能夠輕鬆地分析和視覺化地理資料。
        """

        solara.Markdown(markdown)

