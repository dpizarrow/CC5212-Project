import sys
from pyspark.sql import SparkSession

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: sample.py <filein> <fileout>", file=sys.stderr)
        sys.exit(-1)
    