from pyspark.sql import SparkSession
import json
import os

# Crear carpeta 'results' si no existe
os.makedirs("results", exist_ok=True)

def generar_json_mongo(df):
    # Filtro para MongoDB: Año 2024 y Value > 1000
    df_filtered = df.filter((df.Date.contains("2024")) & (df.Value > 1000))
    results = df_filtered.toJSON().collect()
    with open("results/dataMongo.json", "w") as f:
        json.dump(results, f)

def generar_json_pgsql(df):
    # Filtro para PostgreSQL: frontera US-Canada Border y tipo Trucks
    df_filtered = df.filter((df.Border == "US-Canada Border") & (df.Measure == "Trucks"))
    results = df_filtered.toJSON().collect()
    with open("results/dataPgsql.json", "w") as f:
        json.dump(results, f)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("border-crossings").getOrCreate()

    print("Leyendo border.csv ...")
    df_border = spark.read.csv("border.csv", header=True, inferSchema=True)

    df_border.createOrReplaceTempView("border")

    # Llamar a las funciones para generar los archivos JSON
    generar_json_mongo(df_border)
    generar_json_pgsql(df_border)

    print("Archivos JSON generados exitosamente en la carpeta 'results'.")
    spark.stop()
    print("Spark detenido.")