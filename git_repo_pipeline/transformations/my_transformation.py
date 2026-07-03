"""
Pipeline dummy funcțional cu date generate
"""
from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Bronze Layer - Date dummy generate
@dp.materialized_view(
    comment="Date dummy de vânzări generate"
)
def bronze_sales():
    return (
        spark.range(1000)
        .withColumn("sale_id", F.col("id"))
        .withColumn("product", F.expr("concat('Product_', cast(id % 10 as string))"))
        .withColumn("quantity", F.expr("cast(rand() * 100 as int) + 1"))
        .withColumn("price", F.expr("round(rand() * 1000 + 10, 2)"))
        .withColumn("sale_date", F.expr("date_sub(current_date(), cast(rand() * 365 as int))"))
        .withColumn("customer_id", F.expr("cast(rand() * 100 as int) + 1"))
        .drop("id")
    )

# Silver Layer - Date curățate cu validări
@dp.table(
    comment="Date de vânzări validate și curățate"
)
@dp.expect_or_drop("valid_quantity", "quantity > 0")
@dp.expect_or_drop("valid_price", "price > 0")
def silver_sales():
    return (
        spark.read.table("bronze_sales")
        .withColumn("total_amount", F.col("quantity") * F.col("price"))
        .withColumn("processed_at", F.current_timestamp())
    )

# Gold Layer - Agregări pentru raportare
@dp.materialized_view(
    comment="Vânzări agregate pe produs"
)
def gold_sales_by_product():
    return (
        spark.read.table("silver_sales")
        .groupBy("product")
        .agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("price").alias("avg_price"),
            F.count("sale_id").alias("number_of_sales")
        )
        .orderBy(F.desc("total_revenue"))
    )
