# Can optionally do some basic pre-processing and cleanup
df = spark.table("workspace.raw_sftp.claims")
df_selected = df.select(
    "claim_id", "date_of_loss", "body_part", "is_litigated", "number_of_priors"
).filter((df.claim_id.isNotNull()) & (df.claim_id != ""))
df_selected.write.format("delta").option("overwriteSchema", "true").mode(
    "overwrite"
).saveAsTable("medallion.0_landing.claims")
