from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

spark.sql("CREATE SCHEMA IF NOT EXISTS service_desk.silver")

spark.sql(""" \
CREATE OR REPLACE TABLE service_desk.silver.customers_clean AS
SELECT
    customer_id,
    trim(customer_name) AS customer_name,
    trim(department) AS department,
    trim(region) AS region,
    trim(customer_type) AS customer_type,
    lower(trim(email)) AS email,
    _ingested_at
FROM service_desk.bronze.customers
WHERE customer_id IS NOT NULL
""")

spark.sql(""" 
CREATE OR REPLACE TABLE service_desk.silver.departments_clean AS
SELECT *
FROM service_desk.bronze.departments
""")

spark.sql(""" 
CREATE OR REPLACE TABLE service_desk.silver.employees_clean AS
SELECT *
FROM service_desk.bronze.employees
""")

spark.sql(""" 
CREATE OR REPLACE TABLE service_desk.silver.services_clean AS
SELECT 
    service_id,
    trim(service_name) AS service_name,
    trim(service_domain) AS service_domain,
    trim(owner_department) AS owner_department,
    trim(business_unit) AS business_unit,
    cast(target_availability_pct AS double) AS target_availability_pct,
    trim(criticality) AS criticality,
    _ingested_at
FROM service_desk.bronze.services
WHERE service_id IS NOT NULL
""")

spark.sql(""" 
CREATE OR REPLACE TABLE service_desk.silver.tickets_clean AS
SELECT 
    ticket_id,
    cast(opened_at AS timestamp) AS opened_at,
    cast(closed_at AS timestamp) AS closed_at,
    trim(status) AS status,
    trim(priority) AS priority,
    trim(category) AS category,
    trim(channel) AS channel,
    customer_id,
    service_id,
    assigned_agent_id,
    CASE
          WHEN closed_at IS NOT NULL
          THEN round((unix_timestamp(cast(closed_at AS timestamp)) - unix_timestamp(cast(opened_at AS timestamp))) / 3600, 2)
          ELSE NULL
    END AS resolution_hours
FROM service_desk.bronze.tickets_raw
WHERE ticket_id IS NOT NULL
""")

print("Silver tables created successfully")


