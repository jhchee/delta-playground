WITH incoming_df_scd ( -- Append incoming df with SCD columns
    SELECT
        *,
        current_timestamp() AS commit_time, -- Indicate when this is ingested
        md5(concat(${attributes})) AS checksum -- Checksum used to prevent duplicate values
    FROM incoming_df
)
MERGE INTO ${target_table} scd_table
USING incoming_df_scd
ON scd_table.${merge_key} = incoming_df_scd.${merge_key}
WHEN MATCHED AND incoming_df_scd.checksum != scd_table.checksum
THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *