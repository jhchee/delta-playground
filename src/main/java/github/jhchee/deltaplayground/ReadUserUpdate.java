package github.jhchee.deltaplayground;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.concurrent.TimeoutException;

public class ReadUserUpdate extends DeltaWrapper {

    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
        SparkSession spark = getSession("ReadUserUpdate");

        Dataset<Row> df = spark.readStream()
                               .format("delta")
                               .option("readChangeFeed", "true")
                               .option("startingVersion", 0)
                               .table("default.user");

        df.writeStream()
          .format("console")
          .start()
          .awaitTermination();
    }
}

