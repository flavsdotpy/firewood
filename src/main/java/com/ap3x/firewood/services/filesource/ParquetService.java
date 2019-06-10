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

@Service(value = "parquetService")
public class ParquetService implements FileSourceService {

    private static final Log LOGGER = LogFactory.getLog(ParquetService.class);

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Autowired
    private Environment env;

    @Override
    public Boolean write(Dataset<Row> dataset, String output) {
        LOGGER.debug("write() - Writing data to parquet: " + output);
        dataset.write().mode(SaveMode.Append).parquet(output);
        return true;
    }

    @Override
    public Dataset<Row> read(String... inputs) {
        LOGGER.debug("read() - Reading parquet from: " + Arrays.toString(inputs));
        return firewoodSpark.getSparkSession().read().parquet(inputs);
    }
}
