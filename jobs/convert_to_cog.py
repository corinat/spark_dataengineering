import logging
import sys
from pathlib import Path

from data_transformations.raster import cog

LOG_FILENAME = "project.log"

if __name__ == "__main__":
    logging.basicConfig(filename=LOG_FILENAME, level=logging.INFO)

    if len(sys.argv) != 3:
        print("Usage: python jobs/convert_to_cog.py <INPUT_TIFF> <OUTPUT_COG>")
        sys.exit(1)

    input_tiff = sys.argv[1]
    output_cog = sys.argv[2]

    Path(output_cog).parent.mkdir(parents=True, exist_ok=True)
    cog.convert_to_cog(input_tiff, output_cog)
