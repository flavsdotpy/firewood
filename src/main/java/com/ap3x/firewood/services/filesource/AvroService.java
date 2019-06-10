package com.ap3x.firewood.services.filesource;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Arrays;

@Service(value = "avroService")
public class AvroService implements FileSourceService {

    private static final Log LOGGER = LogFactory.getLog(AvroService.class);

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Autowired
    private Environment env;

    @Override
    public Boolean write(Dataset<Row> dataset, String output) {
        LOGGER.debug("write() - Writing data to avro: " + output);
        dataset.write()
                .mode(SaveMode.Append)
                .format("com.databricks.spark.avro")
                .option("recordName", "AvroFaio")
                .option("recordNamespace", "br.com.rivendel.bigdata")
                .save(output);
        return true;
    }

    @Override
    public Dataset<Row> read(String... inputs) {
        LOGGER.debug("read() - Reading avro from: " + Arrays.toString(inputs));
        return firewoodSpark.getSparkSession().read().format("com.databricks.spark.avro").load(inputs);
    }
}
