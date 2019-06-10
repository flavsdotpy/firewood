package com.ap3x.firewood.services.streamsource;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;
import scala.Tuple2;

public interface StreamingSourceService {

    Tuple2 read() throws ClassNotFoundException;
    Boolean write(final JavaRDD<Row> rdd, final String output) throws ClassNotFoundException;
}
