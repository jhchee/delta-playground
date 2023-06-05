package github.jhchee.deltaplayground;

import github.jhchee.deltaplayground.schema.UserScdType1;
import github.jhchee.deltaplayground.util.MockDataUtil;
import github.jhchee.deltaplayground.util.ScdType1Util;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

public class WriteScdType1 extends DeltaWrapper {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = getSession("WriteScdType1");
        UserScdType1.createTableIfNotExists(spark);

        String mergeKey = "user_id";
        List<String> attributes = Arrays.asList("city", "salary");

        Dataset<Row> df1 = MockDataUtil.firstBatch(spark);
        ScdType1Util.upsert(df1, UserScdType1.TABLE_NAME, mergeKey, attributes);

        Dataset<Row> df2 = MockDataUtil.secondBatch(spark);
        ScdType1Util.upsert(df2, UserScdType1.TABLE_NAME, mergeKey, attributes);
    }
}

