import pyspark.sql


def main():
    spark = (
        pyspark.sql.SparkSession
        .builder
        .master("local[2]")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,io.delta:delta-core_2.12:2.2.0,io.delta:delta-iceberg_2.12:3.0.0rc1")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.delta_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.catalog.sandbox", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.sandbox.type", "rest")
        .config("spark.sql.catalog.sandbox.uri", "http://localhost:8181")
        .config("spark.sql.catalog.sandbox.warehouse", "sandbox")
        .config("spark.sql.catalog.sandbox.credential", "mike:test1")
        .config("spark.sql.defaultCatalog", "sandbox")
        .config("spark.sql.catalog.sandbox.default-namespace", "sandbox")
        .getOrCreate()
    )
    # r = spark.range(10)
    # r.write.mode("OVERWRITE").saveAsTable("tbl1")

    # r = spark.range(10)
    # r.write.mode("OVERWRITE").format("delta").saveAsTable("tbl2")
    
    print(spark.sql("show tables").collect())
    print(spark.sql("show create table tbl1").collect()[0][0])
    # print(spark.sql("show create table tbl2").collect()[0][0])
    print(spark.sql("select * from sandbox.sandbox.tbl1.history"))
    print(spark.sql("show databases").collect())
    

if __name__ == "__main__":
    main()