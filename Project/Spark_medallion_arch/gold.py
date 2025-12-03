from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType, TimestampType, FloatType
import pyspark.sql.functions as f
catelog_name = 'ecom'


df_products = spark.read.table(f'{catelog_name}.silver.slv_products')
df_brand = spark.read.table(f'{catelog_name}.silver.slv_brands')
df_category = spark.read.table(f'{catelog_name}.silver.slv_category')

df_products.createOrReplaceTempView('v_products')
df_brand.createOrReplaceTempView('v_brands')
df_category.createOrReplaceTempView('v_category')

display(spark.sql('select * from v_products limit 5'))

spark.sql(f'use catalog {catelog_name}')

%sql
create or replace table gold.gold_dim_product as

with brand_categories as (
  select
  b.brand_name,
  b.brand_code,
  c.category_name,
  c.category_code
  from v_brands as b 
  join v_category as c 
  on b.category_code = c.category_code
)

select
p.product_id,
p.sku,
p.category_code,
coalesce(bc.category_name, 'Not Available') as category_name,
p.brand_code,
coalesce(bc.brand_name, 'Not Available') as brand_name,
p.color,
p.size,
p.material,
p.weight_grams,
p.length_cm,
p.width_cm,
p.height_cm,
p.rating_count,
p.file_name,
p.ingest_timestamp
from v_products as p
left join brand_categories as bc
on p.brand_code = bc.brand_code

# India states
india_region = {
    "MH": "West", "GJ": "West", "RJ": "West",
    "KA": "South", "TN": "South", "TS": "South", "AP": "South", "KL": "South",
    "UP": "North", "WB": "North", "DL": "North"
}
# Australia states
australia_region = {
    "VIC": "SouthEast", "WA": "West", "NSW": "East", "QLD": "NorthEast"
}

# United Kingdom states
uk_region = {
    "ENG": "England", "WLS": "Wales", "NIR": "Northern Ireland", "SCT": "Scotland"
}

# United States states
us_region = {
    "MA": "NorthEast", "FL": "South", "NJ": "NorthEast", "CA": "West", 
    "NY": "NorthEast", "TX": "South"
}

# UAE states
uae_region = {
    "AUH": "Abu Dhabi", "DU": "Dubai", "SHJ": "Sharjah"
}

# Singapore states
singapore_region = {
    "SG": "Singapore"
}

# Canada states
canada_region = {
    "BC": "West", "AB": "West", "ON": "East", "QC": "East", "NS": "East", "IL": "Other"
}

# Combine into a master dictionary
country_state_map = {
    "India": india_region,
    "Australia": australia_region,
    "United Kingdom": uk_region,
    "United States": us_region,
    "United Arab Emirates": uae_region,
    "Singapore": singapore_region,
    "Canada": canada_region
}  

# 1 Flatten country_state_map into a list of Rows
from pyspark.sql import Row
rows = []
for country, states in country_state_map.items():
    for state_code, region in states.items():
        rows.append(Row(country=country, state=state_code, region=region))
rows[:10]        

# 2️ Create mapping DataFrame
df_region_mapping = spark.createDataFrame(rows)

# Optional: show mapping
df_region_mapping.show(truncate=False)

df_silver = spark.table(f'{catelog_name}.silver.slv_customers')
display(df_silver.limit(5))

df_gold = df_silver.join(df_region_mapping, on=['country', 'state'], how='left')

df_gold = df_gold.fillna({'region': 'Other'})

display(df_gold.limit(5))

# Write raw data to the gold layer (catalog: ecommerce, schema: gold, table: gld_dim_customers)
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catelog_name}.gold.gld_dim_customers")

df_silver = spark.table(f'{catelog_name}.silver.slv_calendar')
display(df_silver.limit(5))

df_gold = df_silver.withColumn("date_id", f.date_format(f.col("date"), "yyyyMMdd").cast("int"))

# Add month name (e.g., 'January', 'February', etc.)
df_gold = df_gold.withColumn("month_name", f.date_format(f.col("date"), "MMMM"))

# Add is_weekend column
df_gold = df_gold.withColumn(
    "is_weekend",
    f.when(f.col("day_name").isin("Saturday", "Sunday"), 1).otherwise(0)
)

display(df_gold.limit(5))


desired_columns_order = ["date_id", "date", "year", "month_name", "day_name", "is_weekend", "quarter", "week", "_ingested_at", "_source_file"]

df_gold = df_gold.select(desired_columns_order)

display(df_gold.limit(5))

# write table to gold layer
df_gold.write.format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable(f"{catelog_name}.gold.gld_dim_date")

%sql

DESCRIBE EXTENDED ecom.gold.gld_dim_date;


