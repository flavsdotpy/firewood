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

@Service(value = "orcService")
public class OrcService implements FileSourceService {

    private static final Log LOGGER = LogFactory.getLog(OrcService.class);

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Autowired
    private Environment env;

    @Override
    public Boolean write(Dataset<Row> dataset, String output) {
        LOGGER.debug("write() - Writing orc to: " + output);
        dataset.write().mode(SaveMode.Append).orc(output);
        return true;
    }

    @Override
    public Dataset<Row> read(String... inputs) {
        LOGGER.debug("read() - Reading from orc: " + Arrays.toString(inputs));
        return firewoodSpark.getSparkSession().read().orc(inputs);
    }
}
