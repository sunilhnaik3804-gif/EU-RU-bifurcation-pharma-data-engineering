hcp_df = hcp_df.withColumn(
    "hcp_name",
    F.upper(F.trim(F.col("hcp_name")))
)
