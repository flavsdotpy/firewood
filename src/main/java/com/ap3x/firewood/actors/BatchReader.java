package com.ap3x.firewood.actors;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.exceptions.InvalidParamsException;
import com.ap3x.firewood.services.dbsource.DBSourceService;
import com.ap3x.firewood.services.filesource.FileSourceService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component(value = "batchReader")
public class BatchReader {

    private static final Log LOGGER = LogFactory.getLog(BatchReader.class);

    @Autowired
    private Environment env;

    @Autowired
    private FirewoodContext faio;

    public Dataset<Row> read(final String readString) {
        final String readSourceType = env.getProperty("input.type");

        if (readSourceType.equals("db"))
            return readFromDBSources(readString);
        if (readSourceType.equals("file"))
            return readFromFileSources(readString);

        throw new InvalidParamsException("Resource type not set or invalid: " + readSourceType);
    }

    private Dataset<Row> readFromDBSources(String readString) {
        LOGGER.debug("readFromDBSources() - Reading from DB sources");
        return faio.getBean(
                env.getProperty("input.engine").concat("Service"),
                DBSourceService.class
        ).read(readString, env.getProperty("input.location"));
    }

    private Dataset<Row> readFromFileSources(String readString) {
        LOGGER.debug("readFromFileSources() - Reading from file sources");
        return faio.getBean(
                env.getProperty("input.format").concat("Service"),
                FileSourceService.class
        ).read(readString.split(","));
    }
}
