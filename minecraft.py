from pyspark.sql import SparkSession
import json
import os

# Crear carpeta 'results' si no existe
os.makedirs("results", exist_ok=True)

def generar_json_mongo(df):
    # Filtro para objetos de rareza "Común" y dimensión "Overworld"
    df_filtered = df.filter((df.Rareza == "Común") & (df.Dimensión == "Overworld"))

    # Guardar como JSON plano con .part y _SUCCESS
    output_dir = "results/mongo_json_output"
    df_filtered.write.mode("overwrite").json(output_dir)

    # Guardar como archivo .json legible (opcional)
    results = df_filtered.toJSON().collect()
    with open("results/dataMongo.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

def generar_json_pgsql(df):
    # Filtro para objetos de rareza "Raro" y etapa del juego "Medio"
    df_filtered = df.filter((df.Rareza == "Raro") & (df["Etapa del juego"] == "Medio"))

    # Guardar como JSON plano con .part y _SUCCESS
    output_dir = "results/pgsql_json_output"
    df_filtered.write.mode("overwrite").json(output_dir)

    # Guardar como archivo .json legible (opcional)
    results = df_filtered.toJSON().collect()
    with open("results/dataPgsql.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    spark = SparkSession.builder.appName("minecraft-items").getOrCreate()

    print("Leyendo minecraft.csv ...")
    df_minecraft = spark.read.csv("minecraft.csv", header=True, inferSchema=True)

    df_minecraft.createOrReplaceTempView("minecraft")

    generar_json_mongo(df_minecraft)
    generar_json_pgsql(df_minecraft)

    print("Archivos JSON y .part generados en 'results/'")
    spark.stop()
    print("Spark detenido.")
