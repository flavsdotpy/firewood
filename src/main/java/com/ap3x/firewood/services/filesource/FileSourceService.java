package com.ap3x.firewood.services.filesource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface FileSourceService {

    Boolean write(final Dataset<Row> dataset, final String output);
    Dataset<Row> read(final String... inputs);

}
