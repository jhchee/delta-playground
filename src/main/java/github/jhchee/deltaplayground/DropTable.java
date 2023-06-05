package github.jhchee.deltaplayground;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;

public class DropTable extends DeltaWrapper {

    public static void main(String[] args) {
        SparkSession spark = getSession("DropTable");

        spark.sql("DROP TABLE IF EXISTS default.user_scd_type_1");
        spark.sql("DROP TABLE IF EXISTS default.user_scd_type_2");
    }
}

