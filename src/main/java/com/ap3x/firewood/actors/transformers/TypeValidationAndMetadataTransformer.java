package com.ap3x.firewood.actors.transformers;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TypeValidationAndMetadataTransformer implements Transformer {

    @Autowired
    private MetadataTransformer metadataTransformer;

    @Autowired
    private TypeValidationTransformer typeValidationTransformer;

    @Override
    public Dataset<Row> transform(final FirewoodSpark spark, final Dataset<Row> dataset, final String origin,
                                  final StructType schema, final Date exportedAt, final Date curDate) {
        final Dataset<Row> validatedDataset = typeValidationTransformer
                .transform(spark, dataset, null, schema, null, null);

        final Dataset<Row> medatadaDataset = metadataTransformer
                .transform(spark, validatedDataset, origin, null, exportedAt, curDate);

        return medatadaDataset;
    }

}
