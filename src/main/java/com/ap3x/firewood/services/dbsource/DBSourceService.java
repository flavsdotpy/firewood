package com.ap3x.firewood.services.dbsource;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface DBSourceService {

    Dataset<Row> read(final String query, final String source);
    Boolean write(final Dataset<Row> dataset, final String destination);
}
