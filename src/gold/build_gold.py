from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    when,
    sum as spark_sum,
    round

)


spark = SparkSession.builder.getOrCreate()

catalog = "service_desk"

tickets = spark.table(f"{catalog}.silver.tickets_clean")
customers = spark.table(f"{catalog}.silver.customers_clean")
employees = spark.table(f"{catalog}.silver.employees_clean")
services = spark.table(f"{catalog}.silver.services_clean")

gold = (
    tickets.alias("t")

    .join(
        customers.alias("c"),
        col("t.customer_id") == col("c.customer_id"),
        "left"  
    )
    .join(
        services.alias("s"),
        col("t.service_id") == col("s.service_id"),
        "left"
    )

    .join(
        employees.alias("e"),
        col("t.assigned_agent_id") == col("e.employee_id"),
        "left"
    )

    .select(
        col("t.ticket_id"),
        col("t.assigned_agent_id"),
        col("s.service_id"),
        col("t.opened_at"),
        col("t.closed_at"),
        
        col("t.status"),
        col("t.priority"), 
        col("t.category"),
        col("t.channel"),

        col("t.resolution_hours"),

        col("c.customer_name"),
        col("c.department").alias("customer_department"),
        col("c.region").alias("customer_region"),
        col("c.customer_type"),

        col("s.service_name"),
        col("s.service_domain"),
        col("s.business_unit"),
        col("s.criticality"),  

        col("e.employee_name").alias("assigned_agent"),
        col("e.role").alias("agent_role"),
        col("e.department").alias("agent_department"),
        col("e.region").alias("agent_region"),
    )
)

agent_performance = (
    gold
    .groupBy(
        ("assigned_agent_id"),
        ("assigned_agent"),
        ("agent_role"),
        ("agent_department"),
        ("agent_region")
    )
    .agg(
        count("ticket_id").alias("total_tickets"),
        
        spark_sum(
            when(col("status") == "Closed", 1).otherwise(0)
        ).alias("closed_tickets"),

        round(
            avg("resolution_hours"),
            2
        ).alias("avg_resolution_hours")
    )
)

service_statistic = (
    gold
    .groupBy(
        ("service_id"),
        ("service_name"),
        ("service_domain"),
        ("customer_department"),
        ("business_unit"),
        ("criticality")
    )
    .agg(
        count("ticket_id").alias("total_tickets"),

        spark_sum(
            when(col("status") == "Closed", 1).otherwise(0)
        ).alias("closed_tickets"),

    round(
        avg("resolution_hours"),
        2
    ).alias("avg_resolution_hours")
  )
)

agent_performance.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.agent_performance")


gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.tickets_summary")

service_statistic.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{catalog}.gold.service_statistic")

print("Gold table created successfully.")

