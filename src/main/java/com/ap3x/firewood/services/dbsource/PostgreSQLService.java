package com.ap3x.firewood.services.dbsource;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service(value = "pgsqlService")
public class PostgreSQLService implements DBSourceService {

    private static final Log LOGGER = LogFactory.getLog(PostgreSQLService.class);

    @Autowired
    private Environment env;

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Override
    public Dataset<Row> read(final String query, final String source) {
        final String finalQuery = String.format("(%s) as dataframe", query);
        LOGGER.debug("read() - Executing query at PostgreSQL: " + query);
        return firewoodSpark.getSparkSession().read()
                .format("jdbc")
                .option("driver", "org.postgresql.Driver")
                .option("url", env.getProperty("input.url"))
                .option("dbtable", finalQuery)
                .option("user", env.getProperty("input.user"))
                .option("password", env.getProperty("input.pass"))
                .load();
    }

    @Override
    public Boolean write(Dataset<Row> dataset, String destination) {
        throw new RuntimeException("Not yet implemented!");
    }
}
