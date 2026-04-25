from unittest.mock import MagicMock

from data_transformations.raster import zonal_stats


def test_load_zones_uses_geopackage_format() -> None:
    mock_spark = MagicMock()

    zonal_stats.load_zones(mock_spark, "some/path.gpkg")

    mock_spark.read.format.assert_called_once_with("geopackage")


def test_load_zones_sets_default_table_name() -> None:
    mock_spark = MagicMock()

    zonal_stats.load_zones(mock_spark, "some/path.gpkg")

    mock_spark.read.format().option.assert_called_once_with("tableName", "clip")


def test_load_zones_sets_custom_table_name() -> None:
    mock_spark = MagicMock()

    zonal_stats.load_zones(mock_spark, "some/path.gpkg", table_name="zones")

    mock_spark.read.format().option.assert_called_once_with("tableName", "zones")


def test_load_zones_loads_from_given_path() -> None:
    mock_spark = MagicMock()

    zonal_stats.load_zones(mock_spark, "resources/clip.gpkg")

    mock_spark.read.format().option().load.assert_called_once_with("resources/clip.gpkg")


# ---------------------------------------------------------------------------
# zonal_stats.compute_zonal_stats
# ---------------------------------------------------------------------------

def test_compute_zonal_stats_registers_raster_temp_view() -> None:
    mock_spark = MagicMock()
    mock_raster_df = MagicMock()
    mock_zones_df = MagicMock()

    zonal_stats.compute_zonal_stats(mock_spark, mock_raster_df, mock_zones_df)

    mock_raster_df.createOrReplaceTempView.assert_called_once_with("raster_data")


def test_compute_zonal_stats_registers_zones_temp_view() -> None:
    mock_spark = MagicMock()
    mock_raster_df = MagicMock()
    mock_zones_df = MagicMock()

    zonal_stats.compute_zonal_stats(mock_spark, mock_raster_df, mock_zones_df)

    mock_zones_df.createOrReplaceTempView.assert_called_once_with("zones")


def test_compute_zonal_stats_sql_contains_all_stat_types() -> None:
    mock_spark = MagicMock()
    mock_raster_df = MagicMock()
    mock_zones_df = MagicMock()

    zonal_stats.compute_zonal_stats(mock_spark, mock_raster_df, mock_zones_df)

    sql_query = mock_spark.sql.call_args[0][0]
    for stat in ("count", "sum", "mean", "stddev", "min", "max"):
        assert stat in sql_query, f"Expected stat '{stat}' in SQL query"


def test_compute_zonal_stats_sql_contains_output_column_names() -> None:
    mock_spark = MagicMock()
    mock_raster_df = MagicMock()
    mock_zones_df = MagicMock()

    zonal_stats.compute_zonal_stats(mock_spark, mock_raster_df, mock_zones_df)

    sql_query = mock_spark.sql.call_args[0][0]
    for col in ("zone_geometry", "pixel_count", "pixel_sum", "pixel_mean", "pixel_stddev", "pixel_min", "pixel_max"):
        assert col in sql_query, f"Expected column '{col}' in SQL query"



