from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, FloatType
import pyspark.sql.functions as f

catalog_name = 'ecom'
df_bronze = spark.table(f'{catalog_name}.bronze.brz_brand')
df_bronze.show()

df_silver = df_bronze.withColumn('brand_name', f.trim(f.col("brand_name")))
df_silver.show()

df_silver.select("category_code").distinct().show()

anamolies= {
  'GROCERY' : 'GRCY',
  'BOOKS' : 'BKS',
  'TOYS' : 'TOY'  
}
df_silver = df_silver.replace(anamolies, subset=['category_code'])
df_silver.show()

df_silver.select("category_code").distinct().show()

df_silver.write.format('delta') \
    .mode('overwrite') \
        .option('mergeSchema', 'true') \
            .saveAsTable(f'{catalog_name}.silver.slv_brands')
