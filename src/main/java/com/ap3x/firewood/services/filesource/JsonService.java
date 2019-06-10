package com.ap3x.firewood.services.filesource;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service(value = "jsonService")
public class JsonService implements FileSourceService {

    private static final Log LOGGER = LogFactory.getLog(JsonService.class);

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
        final Boolean multiLine = env.getProperty("input.multiLine", Boolean.class);
        LOGGER.debug("readFromJson() - Reading data from: " + inputs);
        return firewoodSpark.getSparkSession().read()
                .option("multiLine", multiLine)
                .json(inputs);
    }
}
