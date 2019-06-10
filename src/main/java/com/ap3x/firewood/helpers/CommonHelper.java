package com.ap3x.firewood.helpers;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

@Component
public class CommonHelper {

    public Map<String, Integer> countLines(final Dataset<Row> dataFrame){
        return dataFrame.javaRDD().mapToPair(row -> {
            if (null == row.getAs("error"))
                return new Tuple2<>("success", 1);
            else
                return new Tuple2<>("error", 2);
        }).reduceByKey((a, b) -> a + b).collectAsMap();
    }

    public Boolean isOlapEntity(final List<String> olapEntities, final String filePath){
        for (String entity : olapEntities){
            if (filePath.contains(entity))
                return true;
        }
        return false;
    }
}
