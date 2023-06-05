package github.jhchee.deltaplayground.util;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

public class MockDataUtil {
    public static final StructType USER_SCHEMA = new StructType().add("user_id", DataTypes.StringType, false)
                                                                 .add("city", DataTypes.StringType, true)
                                                                 .add("salary", DataTypes.StringType, true);

    public static Dataset<Row> firstBatch(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create("9612e6e0-2a7f-4198-a617-ad5b5f8e8307", "Port Lynwood, VT 93558", "3000"),
                RowFactory.create("bf41071e-1d82-4f2c-9078-f22a397d9a29", "South Jonathan", "4000"),
                RowFactory.create("ed7d42b7-5b76-48af-ab7c-f4e364643a7f", "Gloverbury", "9000")
        );
        return spark.createDataFrame(rows, USER_SCHEMA);
    }

    public static Dataset<Row> secondBatch(SparkSession spark) {
        List<Row> rows = Arrays.asList(
                RowFactory.create("9612e6e0-2a7f-4198-a617-ad5b5f8e8307", "Port Lynwood, VT 93558", "5000"), // changed
                RowFactory.create("ed7d42b7-5b76-48af-ab7c-f4e364643a7f", "Gloverbury", "9000") // unchanged
        );
        return spark.createDataFrame(rows, USER_SCHEMA);
    }
}
