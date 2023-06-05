package github.jhchee.deltaplayground.util;

import org.apache.spark.sql.SparkSession;

public class DeltaUtils {
    public static boolean tableExists(SparkSession spark, String tableName) {
        try {
            spark.read().table(tableName);
            return true;
        } catch (Exception exception) {
            return false;
        }
    }
}
