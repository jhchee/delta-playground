package github.jhchee.deltaplayground.schema;

import io.delta.tables.DeltaTable;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class UserScdType2 {
    public static final StructType SCHEMA = root();
    public static final String TABLE_NAME = "user_scd_type_2";
    public static final String LOCATION = "s3a://spark/user_scd_type_2/";

    public static StructType root() {
        StructType root = new StructType();
        root = root.add("user_id", DataTypes.StringType);
        root = root.add("city", DataTypes.StringType);
        root = root.add("salary", DataTypes.StringType);
        root = root.add("is_current", DataTypes.BooleanType);
        root = root.add("valid_from", DataTypes.TimestampType);
        root = root.add("valid_to", DataTypes.TimestampType);
        root = root.add("checksum", DataTypes.StringType);
        return root;
    }

    public static void createTableIfNotExists(SparkSession spark) {
        DeltaTable.createIfNotExists(spark)
                  .tableName(TABLE_NAME)
                  .addColumns(SCHEMA)
                  .location(LOCATION)
                  .property("delta.enableChangeDataFeed", "true")
                  .execute();
    }
}
