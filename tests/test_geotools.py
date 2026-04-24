from sedona.spark import SedonaContext

config = SedonaContext.builder().master("local").getOrCreate()
sedona = SedonaContext.create(config)

raster_path = "resources/clipped_raster.tif"
vector_path = "resources/clip.gpkg"

# Load the raster (already clipped by clip.gpkg)
sedona.sql(f"""
    CREATE OR REPLACE TEMP VIEW raster_view AS
    SELECT RS_FromGeoTiff(content) AS raster
    FROM binaryFile.`{raster_path}`
""")

# Load the clip vector boundary
sedona.read.format("geopackage").option("tableName", "clip").load(vector_path) \
    .createOrReplaceTempView("clip_vector")

# Inspect metadata: dimensions, number of bands, CRS, envelope
print("=== Raster Metadata ===")
sedona.sql("""
    SELECT
        RS_Width(raster)    AS width,
        RS_Height(raster)   AS height,
        RS_NumBands(raster) AS num_bands,
        RS_Metadata(raster) AS metadata,
        RS_Envelope(raster) AS envelope
    FROM raster_view
""").show(truncate=False)

# Summary statistics per stat type for band 1
print("=== Band 1 Summary Stats ===")
for stat in ("count", "sum", "mean", "stddev", "min", "max"):
    sedona.sql(f"""
        SELECT '{stat}' AS stat, RS_SummaryStats(raster, '{stat}') AS value
        FROM raster_view
    """).show(truncate=False)

# Show the clip vector geometry
print("=== Clip Vector Geometry ===")
sedona.sql("SELECT * FROM clip_vector").show(truncate=False)

# Verify raster envelope intersects the clip geometry
print("=== Raster envelope intersects clip geometry ===")
sedona.sql("""
    SELECT ST_Intersects(RS_Envelope(r.raster), v.geometry) AS intersects
    FROM raster_view r, clip_vector v
""").show(truncate=False)

print("Raster test complete!")
