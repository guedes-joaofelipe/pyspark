import os
import requests
import shutil
import zipfile
from io import BytesIO
from pyspark.sql import SparkSession

OUTPUT_FOLDER = './data/'

def download_unzip_file(url:str, output_folder:str):
    """Downloads and unzip files from url"""
    # Get file from url
    response = requests.get(url)

    # Make sure we have a clean output path
    if os.path.exists(output_folder):
        shutil.rmtree(output_folder)

    os.mkdir(output_folder)

    # extracting the zip file contents
    zipfile.ZipFile(BytesIO(response.content)).extractall(output_folder)


def create_shards(output_folder:str):
    """Reads movieLens original file and creates folder with partitioned shards"""

    spark = SparkSession.Builder().getOrCreate()

    # Reading original files
    df_movies = spark.read\
        .format("csv")\
        .option("header", "true")\
        .load(os.path.join(output_folder, 'ml-25m', 'movies.csv'))

    # Repartitioning and saving partitioned shards
    df_movies\
        .repartition(5)\
        .write\
        .option("header", "true")\
        .csv(os.path.join(OUTPUT_FOLDER, "input"))

    os.system(f'rm -r {os.path.join(OUTPUT_FOLDER, "input")}/.part*')
    os.system(f'rm -r {os.path.join(OUTPUT_FOLDER, "input")}/._SUCCESS*')

    # Deleting original folder
    if os.path.exists(os.path.join(output_folder, 'ml-25m')):
        shutil.rmtree(os.path.join(output_folder, 'ml-25m'))


def main(output_folder:str):
    download_unzip_file(
        "http://files.grouplens.org/datasets/movielens/ml-25m.zip",
        output_folder,
    )

    create_shards(output_folder)

if __name__ == '__main__':
    main(OUTPUT_FOLDER)