from sedona.spark import SedonaContext

config = SedonaContext.builder().master("local").getOrCreate()
sedona = SedonaContext.create(config)

sedona.sql("SELECT ST_Point(1.0, 2.0) AS point").show(truncate=False)
print("Sedona is working!")
