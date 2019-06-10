package com.ap3x.firewood.services.filesource;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Arrays;

import static org.apache.spark.sql.functions.lit;

@Service(value = "csvService")
public class CsvService implements FileSourceService {

    private static final Log LOGGER = LogFactory.getLog(CsvService.class);

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Autowired
    private Environment env;

    @Override
    public Boolean write(Dataset<Row> dataset, String output) {
        throw new RuntimeException("Not yet implemented!");
    }

    @Override
    public Dataset<Row> read(String... inputs) {
        LOGGER.debug("readFromCsv() - Reading data from: " + Arrays.toString(inputs));
        Boolean dms = Boolean.valueOf(env.getProperty("input.dms"));
        if (dms){
            return readFromDMSCsv(inputs[0]);
        }
        String delimiter = env.getProperty("input.delimiter", ",");
        return firewoodSpark.getSparkSession().read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .option("delimiter", delimiter)
                .load(inputs);
    }

    private Dataset<Row> readFromDMSCsv(final String inputDir) {
        String delimiter = env.getProperty("input.delimiter");
        if (inputDir.matches(".*?\\/LOAD[0-9A-Z]+.csv$")){
            return firewoodSpark.getSparkSession().read()
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("delimiter", delimiter)
                    .option("multiLine", "true")
                    .option("escape", "\"")
                    .load(inputDir)
                    .withColumn("DB_ACTION", lit("L"));
        } else {
            return firewoodSpark.getSparkSession().read()
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .option("delimiter", delimiter)
                    .option("multiLine", "true")
                    .option("escape", "\"")
                    .load(inputDir)
                    .withColumnRenamed("Op", "DB_ACTION");
        }
    }
}
