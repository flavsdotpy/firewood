package com.ap3x.firewood.actors.transformers;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.*;
import org.springframework.stereotype.Component;
import scala.collection.JavaConversions;
import scala.collection.Map;

import java.io.Serializable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@Component
public class TypeValidationTransformer implements Transformer, Serializable {

    private final Log LOGGER = LogFactory.getLog(TypeValidationTransformer.class);

    private String error = null;

    @Override
    public Dataset<Row> transform(final FirewoodSpark spark,
                                  final Dataset<Row> dataset,
                                  final String notUsedString,
                                  final StructType schema,
                                  final Date notUsedDate,
                                  final Date notUsedDateB) {

        LOGGER.debug("validateTypeAndWriteError() - Validating rows");
        JavaRDD<Row> populatedRows = dataset.javaRDD().map(row -> validateRowAndWriteErrors(
                row, schema
        ));

        LOGGER.debug("validateTypeAndWriteError() - Building dataframe with new error columun");
        Dataset<Row> newDataset = spark.getSparkSession().sqlContext()
                .createDataFrame(populatedRows, buildNewSchema(schema).asNullable()).toDF();

        return newDataset;
    }

    private Row validateRowAndWriteErrors(final Row row, final StructType schema){
        this.error = null;
        Map<String, String> columnValueMap = row.getValuesMap(
                JavaConversions.asScalaBuffer(Arrays.asList(row.schema().fieldNames()))
        );

        List<Object> rowResults = new ArrayList<>();
        for (StructField field : schema.fields()){
            rowResults.add(validateField(columnValueMap, field.name(), field.nullable(), field.dataType()));
        }

        StructType structTypeWithError = buildNewSchema(schema);
        rowResults.add(error);

        return new GenericRowWithSchema(rowResults.toArray(), structTypeWithError);
    }

    private StructType buildNewSchema(final StructType schema){
        if (!Arrays.asList(schema.fieldNames()).contains("error"))
            return schema.add("error", DataTypes.StringType);
        return schema;
    }
    private void buildErrorMessage(final String error){
        if (this.error == null)
            this.error = error.concat("; ");
        else
            this.error = this.error.concat(error).concat("; ");
    }

    private Object validateField(final Map<String, String> schemaMap,
                                 final String column,
                                 final Boolean nullable,
                                 final DataType fieldType) {
        if (!schemaMap.contains(column)) {
            buildErrorMessage("Column ".concat(column).concat(" is at schema but not at file!"));
            return null;
        }
        else if (schemaMap.get(column).get() == null && !nullable) {
            buildErrorMessage("Column ".concat(column).concat(" has a null value!"));
            return null;
        }
        else {
            return validateValueAndType(column, schemaMap.get(column).get(), fieldType);
        }
    }

    private Object validateValueAndType(final String column, final String data, final DataType fieldType){
        try {
            if (data == null)
                return data;
            else if (fieldType instanceof IntegerType)
                return Integer.parseInt(data);
            else if (fieldType instanceof BooleanType)
                return Boolean.valueOf(data);
            else if (fieldType instanceof DoubleType)
                return Double.parseDouble(data);
            else if (fieldType instanceof FloatType)
                return Float.parseFloat(data);
            else if (fieldType instanceof LongType)
                return Long.parseLong(data);
            else if (fieldType instanceof TimestampType)
                return Timestamp.valueOf(data);
            else if (fieldType instanceof DateType){
                return new SimpleDateFormat("dd-MM-yyyy").parse(data);
            }
            return data;
        } catch (Exception e){
            buildErrorMessage("Value "
                    .concat(data)
                    .concat(" could not be cast to ")
                    .concat(fieldType.getClass().getName())
                    .concat(" at column: ")
                    .concat(column));
            return null;
        }
    }
}
