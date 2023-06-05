package github.jhchee.deltaplayground;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

public class ScdUpdate extends DeltaWrapper {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = getSession("ScdUpdate");

        List<Row> rows = Arrays.asList(
                RowFactory.create("9612e6e0-2a7f-4198-a617-ad5b5f8e8307", "Mike", "Smith"),
                RowFactory.create(UUID.randomUUID().toString(), "Bob", "Clark"),
                RowFactory.create(UUID.randomUUID().toString(), null, null)
        );

        DeltaTable.createIfNotExists(spark)
                  .tableName("user_scd")
                  .addColumn("user_id", DataTypes.StringType)
                  .addColumn("valid_from", DataTypes.TimestampType)
                  .addColumn("valid_to", DataTypes.TimestampType)
                  .addColumn("is_current", DataTypes.BooleanType)
                  .addColumn("checksum", DataTypes.StringType)
                  .addColumn("first_name", DataTypes.StringType)
                  .addColumn("last_name", DataTypes.StringType)
                  .location("s3a://spark/user_scd/")
                  .execute();

        StructType schema = new StructType().add("user_id", DataTypes.StringType, false)
                                            .add("first_name", DataTypes.StringType, true)
                                            .add("last_name", DataTypes.StringType, true);

        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df = df.selectExpr(
                "*",
                "current_timestamp() AS valid_from",
                "true AS is_current",
                "null AS valid_to",
                "md5(concat(first_name, last_name)) AS checksum"
        );
        df.createOrReplaceTempView("updates");


        String stageUpdateQuery =  "SELECT NULL AS merge_key, updates.* " +
                            "FROM updates " +
                            "INNER JOIN user_scd " +
                            "ON user_scd.user_id = updates.user_id " +
                            "WHERE user_scd.checksum != updates.checksum " +
                            "AND user_scd.is_current IS true " +
                            "UNION " +
                            "SELECT user_id AS merge_key, * " +
                            "FROM updates";



        // null merge key become insert
        spark.sql("SELECT NULL AS merge_key, updates.* " +
                "FROM updates " +
                "INNER JOIN user_scd " +
                "ON user_scd.user_id = updates.user_id " +
                "WHERE user_scd.checksum != updates.checksum " +
                "AND user_scd.is_current IS true ").show();

        // non-null merge key will try to expire old row
        spark.sql("SELECT user_id AS merge_key, * " +
                "FROM updates").show();

        Dataset<Row> stagedUpdates = spark.sql(stageUpdateQuery);
        stagedUpdates.show();
        stagedUpdates.createOrReplaceTempView("staged_updates");

        spark.sql(
                String.format(
                        "MERGE INTO user_scd AS target "+
                        "USING staged_updates "+
                        "ON target.user_id = staged_updates.merge_key "+
                        "WHEN MATCHED AND staged_updates.checksum != target.checksum AND target.is_current IS true "+
                        "THEN UPDATE SET valid_to = staged_updates.valid_from, is_current = false "+
                        "WHEN NOT MATCHED THEN INSERT * ", stageUpdateQuery)
        );

    }
}

