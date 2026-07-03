from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Tabel simplu de exemplu cu date generate"
)
def date_exemplu():
    """
    Generează un tabel simplu cu 50 de rânduri de date
    """
    return (
        spark.range(50)
        .withColumn("nume", F.concat(F.lit("Client_"), F.col("id")))
        .withColumn("valoare", (F.rand() * 1000).cast("int"))
        .withColumn("categorie", F.when(F.col("id") % 3 == 0, "A")
                                  .when(F.col("id") % 3 == 1, "B")
                                  .otherwise("C"))
        .withColumn("data_creare", F.current_timestamp())
    )
