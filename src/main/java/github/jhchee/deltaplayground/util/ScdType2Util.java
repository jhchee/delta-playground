package github.jhchee.deltaplayground.util;

import org.apache.commons.text.StringSubstitutor;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScdType2Util {
    public static void upsert(Dataset<Row> df, String targetTable, String mergeKey, List<String> attributes) {
        df.createOrReplaceTempView("incoming_df");

        String template = ResourceUtil.readFromFile("sql/ScdType2Template.sql");
        Map<String, Object> params = new HashMap<>();
        params.put("target_table", targetTable);
        params.put("attributes", String.join(",", attributes));
        params.put("merge_key", mergeKey);
        StringSubstitutor substitutor = new StringSubstitutor(params);
        String sqlText = substitutor.replace(template);

        df.sparkSession().sql(sqlText);
    }
}
