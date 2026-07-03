from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(
    comment="Tabel simplu de exemplu"
)
def exemplu_simplu():
    """
    Tabel de exemplu care generează date simple
    """
    return (
        spark.range(100)
        .withColumn("nume", F.concat(F.lit("User_"), F.col("id")))
        .withColumn("valoare", F.rand() * 100)
        .withColumn("timestamp", F.current_timestamp())
    )
