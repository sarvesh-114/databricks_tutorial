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

catalog_name = 'ecom'
df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")

df_bronze.show(10)df_duplicates = df_bronze.groupBy("category_code").count().filter(f.col("count") > 1)
display(df_duplicates)

df_silver = df_bronze.dropDuplicates(['category_code'])
display(df_silver)

df_silver = df_silver.withColumn("category_code", f.upper(f.col("category_code")))
display(df_silver)

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

display(df_bronze.limit(5))

# Check weight_grams column
df_bronze.select("weight_grams").show(5, truncate=False)

# replace 'g' with ''
df_silver = df_bronze.withColumn(
    "weight_grams",
    f.regexp_replace(f.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)

df_silver.select("length_cm").show(3)

# replace , with .
df_silver = df_silver.withColumn(
    "length_cm",
    f.regexp_replace(f.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(3)

df_silver.select("category_code", "brand_code").show(2)

# convert category_code and brand_code to upper case
df_silver = df_silver.withColumn(
    "category_code",
    f.upper(f.col("category_code"))
).withColumn(
    "brand_code",
    f.upper(f.col("brand_code"))
)
df_silver.select("category_code", "brand_code").show(2)

df_silver.select("material").distinct().show()

# Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    f.when(f.col("material") == "Coton", "Cotton")
     .when(f.col("material") == "Alumium", "Aluminum")
     .when(f.col("material") == "Ruber", "Rubber")
     .otherwise(f.col("material"))
)
df_silver.select("material").distinct().show()    

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
catalog_name = 'ecom'
df_bronze = spark.table(f"{catalog_name}.bronze.brz_category")

df_bronze.show(10)
df_duplicates = df_bronze.groupBy("category_code").count().filter(f.col("count") > 1)
display(df_duplicates)
df_silver = df_bronze.dropDuplicates(['category_code'])
display(df_silver)
df_silver = df_silver.withColumn("category_code", f.upper(f.col("category_code")))
display(df_silver)
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_category)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_category")
# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_products")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")
display(df_bronze.limit(5))
# Check weight_grams column
df_bronze.select("weight_grams").show(5, truncate=False)
# replace 'g' with ''
df_silver = df_bronze.withColumn(
    "weight_grams",
    f.regexp_replace(f.col("weight_grams"), "g", "").cast(IntegerType())
)
df_silver.select("weight_grams").show(5, truncate=False)
df_silver.select("length_cm").show(3)
# replace , with .
df_silver = df_silver.withColumn(
    "length_cm",
    f.regexp_replace(f.col("length_cm"), ",", ".").cast(FloatType())
)
df_silver.select("length_cm").show(3)
df_silver.select("category_code", "brand_code").show(2)
# convert category_code and brand_code to upper case
df_silver = df_silver.withColumn(
    "category_code",
    f.upper(f.col("category_code"))
).withColumn(
    "brand_code",
    f.upper(f.col("brand_code"))
)
df_silver.select("category_code", "brand_code").show(2)
df_silver.select("material").distinct().show()
# Fix spelling mistakes
df_silver = df_silver.withColumn(
    "material",
    f.when(f.col("material") == "Coton", "Cotton")
     .when(f.col("material") == "Alumium", "Aluminum")
     .when(f.col("material") == "Ruber", "Rubber")
     .otherwise(f.col("material"))
)
df_silver.select("material").distinct().show()    
df_silver.filter(f.col('rating_count')<0).select("rating_count").show(3)

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    f.when(f.col("rating_count").isNotNull(), f.abs(f.col("rating_count")))
     .otherwise(f.lit(0))  # if null, replace with 0
)
# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")
# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)
null_count = df_bronze.filter(f.col("customer_id").isNull()).count()
null_count
# There are 300 null values in customer_id column. Display some of those
df_bronze.filter(f.col("customer_id").isNull()).show(3)
# Drop rows where 'customer_id' is null
df_silver = df_bronze.dropna(subset=["customer_id"])

# Get row count
row_count = df_silver.count()
print(f"Row count after droping null values: {row_count}")
null_count = df_silver.filter(f.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 
df_silver.filter(f.col("phone").isNull()).show(3)
### Fill null values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# sanity check (If any nulls still exist)
df_silver.filter(f.col("phone").isNull()).show()
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")
# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)
df_bronze.printSchema()
from pyspark.sql.functions import to_date


# Convert the string column to a date type
df_silver = df_bronze.withColumn("date", to_date(df_bronze["date"], "dd-MM-yyyy"))
print(df_silver.printSchema())

df_silver.show(5)
# Find duplicate rows in the DataFrame
duplicates = df_silver.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)
# Remove duplicate rows
df_silver = df_silver.dropDuplicates(['date'])

# Get row count
row_count = df_silver.count()

print("Rows After removing Duplicates: ", row_count)
# Capitalize first letter of each word in day_name
df_silver = df_silver.withColumn("day_name", f.initcap(f.col("day_name")))

df_silver.show(5)
df_silver = df_silver.withColumn("week_of_year", f.abs(f.col("week_of_year")))  # Convert negative to positive

df_silver.show(3)
df_silver = df_silver.withColumn("quarter", f.concat_ws("", f.concat(f.lit("Q"), f.col("quarter"), f.lit("-"), f.col("year"))))

df_silver = df_silver.withColumn("week_of_year", f.concat_ws("-", f.concat(f.lit("Week"), f.col("week_of_year"), f.lit("-"), f.col("year"))))

df_silver.show(3)
# Rename a column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")
# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")

# Convert negative rating_count to positive
df_silver = df_silver.withColumn(
    "rating_count",
    f.when(f.col("rating_count").isNotNull(), f.abs(f.col("rating_count")))
     .otherwise(f.lit(0))  # if null, replace with 0
)

# Check final cleaned data

df_silver.select(
    "weight_grams",
    "length_cm",
    "category_code",
    "brand_code",
    "material",
    "rating_count"
).show(10, truncate=False)

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_dim_products)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_products")

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_customers")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(10)

null_count = df_bronze.filter(f.col("customer_id").isNull()).count()
null_count

# There are 300 null values in customer_id column. Display some of those
df_bronze.filter(f.col("customer_id").isNull()).show(3)

# Drop rows where 'customer_id' is null
df_silver = df_bronze.dropna(subset=["customer_id"])

# Get row count
row_count = df_silver.count()
print(f"Row count after droping null values: {row_count}")

null_count = df_silver.filter(f.col("phone").isNull()).count()
print(f"Number of nulls in phone: {null_count}") 

df_silver.filter(f.col("phone").isNull()).show(3)

### Fill null values with 'Not Available'
df_silver = df_silver.fillna("Not Available", subset=["phone"])

# sanity check (If any nulls still exist)
df_silver.filter(f.col("phone").isNull()).show()

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_customers)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_customers")

# Read the raw data from the bronze table (ecommerce.bronze.brz_calendar)
df_bronze = spark.read.table(f"{catalog_name}.bronze.brz_calendar")

# Get row and column count
row_count, column_count = df_bronze.count(), len(df_bronze.columns)

# Print the results
print(f"Row count: {row_count}")
print(f"Column count: {column_count}")

df_bronze.show(3)

df_bronze.printSchema()

from pyspark.sql.functions import to_date


# Convert the string column to a date type
df_silver = df_bronze.withColumn("date", to_date(df_bronze["date"], "dd-MM-yyyy"))

print(df_silver.printSchema())

df_silver.show(5)

# Find duplicate rows in the DataFrame
duplicates = df_silver.groupBy('date').count().filter("count > 1")

# Show the duplicate rows
print("Total duplicated Rows: ", duplicates.count())
display(duplicates)

# Remove duplicate rows
df_silver = df_silver.dropDuplicates(['date'])

# Get row count
row_count = df_silver.count()

print("Rows After removing Duplicates: ", row_count)

# Capitalize first letter of each word in day_name
df_silver = df_silver.withColumn("day_name", f.initcap(f.col("day_name")))

df_silver.show(5)

df_silver = df_silver.withColumn("week_of_year", f.abs(f.col("week_of_year")))  # Convert negative to positive

df_silver.show(3)

df_silver = df_silver.withColumn("quarter", f.concat_ws("", f.concat(f.lit("Q"), f.col("quarter"), f.lit("-"), f.col("year"))))

df_silver = df_silver.withColumn("week_of_year", f.concat_ws("-", f.concat(f.lit("Week"), f.col("week_of_year"), f.lit("-"), f.col("year"))))

df_silver.show(3)

# Rename a column
df_silver = df_silver.withColumnRenamed("week_of_year", "week")

# Write raw data to the silver layer (catalog: ecommerce, schema: silver, table: slv_calendar)
df_silver.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catalog_name}.silver.slv_calendar")

