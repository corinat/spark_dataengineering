from unittest.mock import MagicMock, patch

from data_transformations.raster import batch_ingest_rasters


def test_load_raster_batch_creates_sedona_context() -> None:
    mock_spark = MagicMock()

    with patch("data_transformations.raster.batch_ingest_rasters.SedonaContext") as mock_sedona_ctx, \
         patch("data_transformations.raster.batch_ingest_rasters.F"):
        batch_ingest_rasters.load_raster_batch(mock_spark, "resources/cog/")

        mock_sedona_ctx.create.assert_called_once_with(mock_spark)


def test_load_raster_batch_reads_binary_file_format() -> None:
    mock_spark = MagicMock()

    with patch("data_transformations.raster.batch_ingest_rasters.SedonaContext") as mock_sedona_ctx, \
         patch("data_transformations.raster.batch_ingest_rasters.F"):
        mock_sedona = mock_sedona_ctx.create.return_value

        batch_ingest_rasters.load_raster_batch(mock_spark, "resources/cog/")

        mock_sedona.readStream.format.assert_called_once_with("binaryFile")


def test_load_raster_batch_filters_tif_files() -> None:
    mock_spark = MagicMock()

    with patch("data_transformations.raster.batch_ingest_rasters.SedonaContext") as mock_sedona_ctx, \
         patch("data_transformations.raster.batch_ingest_rasters.F"):
        mock_sedona = mock_sedona_ctx.create.return_value
        read_stream = mock_sedona.readStream.format.return_value.schema.return_value

        batch_ingest_rasters.load_raster_batch(mock_spark, "resources/cog/")

        options = {
            call.args[0]: call.args[1]
            for call in read_stream.option.call_args_list
        }
        assert options.get("pathGlobFilter") == "*.tif"


def test_load_raster_batch_uses_recursive_lookup() -> None:
    mock_spark = MagicMock()

    with patch("data_transformations.raster.batch_ingest_rasters.SedonaContext") as mock_sedona_ctx, \
         patch("data_transformations.raster.batch_ingest_rasters.F"):
        mock_sedona = mock_sedona_ctx.create.return_value
        read_stream = mock_sedona.readStream.format.return_value.schema.return_value

        batch_ingest_rasters.load_raster_batch(mock_spark, "resources/cog/")

        read_stream.option.return_value.option.assert_called_once_with("recursiveFileLookup", "true")
