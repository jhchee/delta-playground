package github.jhchee.deltaplayground;

import github.jhchee.deltaplayground.schema.UserScdType2;
import github.jhchee.deltaplayground.util.MockDataUtil;
import github.jhchee.deltaplayground.util.ScdType2Util;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class WriteScdType2 extends DeltaWrapper {
    public static void main(String[] args) {
        SparkSession spark = getSession("WriteScdType2");
        UserScdType2.createTableIfNotExists(spark);

        String mergeKey = "user_id";
        List<String> attributes = Arrays.asList("city", "salary");

        Dataset<Row> df1 = MockDataUtil.firstBatch(spark);
        ScdType2Util.upsert(df1, UserScdType2.TABLE_NAME, mergeKey, attributes);

        Dataset<Row> df2 = MockDataUtil.secondBatch(spark);
        ScdType2Util.upsert(df2, UserScdType2.TABLE_NAME, mergeKey, attributes);
    }
}

