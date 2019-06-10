package com.ap3x.firewood.actors.transformers;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.util.Date;

public interface Transformer {

    Dataset<Row> transform(
            final FirewoodSpark spark,
            final Dataset<Row> dataSet,
            final String origin,
            final StructType schema,
            final Date exportedAt,
            final Date curDate
    );
}
