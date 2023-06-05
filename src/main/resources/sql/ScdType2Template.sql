WITH incoming_df_scd ( -- Append incoming df with SCD columns
    SELECT
        *,
        current_timestamp() AS valid_from, -- Indicate when this is ingested
        null AS valid_to, -- Indicate when this is expired
        true AS is_current, -- Indicate whether this is the latest row
        md5(concat(${attributes})) AS checksum -- Checksum used to prevent duplicate values
    FROM incoming_df
), staged_update AS ( -- Preparing data to be merged into the SCD table
    -- This will to insert new row due to expiring SCD row.
    SELECT
    NULL AS merge_key, source.*
    FROM ${target_table} AS scd_table
    INNER JOIN incoming_df_scd as source ON scd_table.${merge_key} = source.${merge_key}
    WHERE scd_table.checksum != source.checksum AND scd_table.is_current IS true -- search for scd row that qualified to be expired
    UNION -- UNION
    -- This will attempt to expire the existing SCD row.
    SELECT ${merge_key} AS merge_key, * FROM incoming_df_scd
)
MERGE INTO ${target_table} scd_table
USING staged_update
ON scd_table.${merge_key} = staged_update.merge_key
WHEN MATCHED AND staged_update.checksum != scd_table.checksum AND scd_table.is_current IS true
THEN UPDATE SET valid_to = staged_update.valid_from, is_current IS false
WHEN NOT MATCHED THEN INSERT *