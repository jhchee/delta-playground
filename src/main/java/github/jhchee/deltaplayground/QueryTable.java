package github.jhchee.deltaplayground;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class QueryTable extends DeltaWrapper {

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = getSession("QueryTable");

        Dataset<Row> df = spark.sql("SELECT * FROM default.user");
        df.show();
    }
}

