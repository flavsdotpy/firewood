package com.ap3x.firewood.actors.transformers;

import com.ap3x.firewood.common.FirewoodSpark;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;
import scala.collection.Map;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class MetadataTransformer implements Transformer, Serializable {

    private static final long serialVersionUID = -993928532258125781L;

    private final Log LOGGER = LogFactory.getLog(MetadataTransformer.class);

    @Override
    public Dataset<Row> transform(final FirewoodSpark spark,
                                  final Dataset<Row> dataSet,
                                  final String filePath,
                                  final StructType notUsedField,
                                  final Date exportedAt,
                                  final Date curDate) {

        final List<Object> metadata = Arrays.asList(filePath, exportedAt.getTime(), curDate.getTime());
        final StructType schema = dataSet.schema();

        LOGGER.debug("transform() - Adding metadata to rows");
        final JavaRDD<Row> populatedRowsRDD = dataSet.javaRDD().map(row -> addMetadata(
                row, schema, metadata
        ));

        LOGGER.debug("validateType() - Building dataSet with new schema");
        final Dataset<Row> dataSetWithMetadata = spark.getSparkSession().sqlContext()
                .createDataFrame(populatedRowsRDD, buildNewSchema(schema).asNullable()).toDF();

        LOGGER.debug("validateType() - Adding rowCount");
        final Dataset<Row> dataSetWithMetadataAndRowCount = dataSetWithMetadata
                .withColumn("rowCount", org.apache.spark.sql.functions.monotonically_increasing_id().plus(1));

        return dataSetWithMetadataAndRowCount;
    }

    private Row addMetadata(final Row row, StructType schema, List<Object> metadata){
        final Map<String, Object> fieldValues = row.getValuesMap(
                JavaConversions.asScalaBuffer(Arrays.asList(schema.fieldNames()))
        );

        final List<Object> values = Lists.newArrayList(fieldValues.values());
        values.addAll(metadata);

        final StructType schemaWithMetadata = buildNewSchema(schema);
        return new GenericRowWithSchema(values.toArray(), schemaWithMetadata);
    }

    private StructType buildNewSchema(final StructType schema) {
        return schema
                .add("origin", DataTypes.StringType)
                .add("origin_timestamp", DataTypes.LongType)
                .add("process_timestamp", DataTypes.LongType);
    }

}
