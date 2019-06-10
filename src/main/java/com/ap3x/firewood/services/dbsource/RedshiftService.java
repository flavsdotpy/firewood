package com.ap3x.firewood.services.dbsource;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

@Service(value = "redshiftService")
public class RedshiftService implements DBSourceService {

    private static final Log LOGGER = LogFactory.getLog(RedshiftService.class);

    @Autowired
    private Environment env;

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Override
    public Dataset<Row> read(final String query, final String location) {
        LOGGER.debug("queryDataFromRedshift() - Executing query: " + query);

        firewoodSpark.getSparkSession().sparkContext()
                .hadoopConfiguration().set("fs.s3.awsAccessKeyId", env.getProperty("awsAccessKeyId"));
        firewoodSpark.getSparkSession().sparkContext()
                .hadoopConfiguration().set("fs.s3.awsSecretAccessKey", env.getProperty("awsSecretAccessKey"));

        return firewoodSpark.getSparkSession().read()
                .format("com.databricks.spark.redshift")
                .option("url", env.getProperty("input.url"))
                .option("tempdir", env.getProperty("input.tempDir"))
                .option("query", query)
                .load();
    }

    @Override
    public Boolean write(final Dataset<Row> dataSet, final String destination) {
        LOGGER.debug("write() - Writing data to Redshift: " + destination);

        firewoodSpark.getSparkSession().sparkContext()
                .hadoopConfiguration().set("fs.s3.awsAccessKeyId", env.getProperty("awsAccessKeyId"));
        firewoodSpark.getSparkSession().sparkContext()
                .hadoopConfiguration().set("fs.s3.awsSecretAccessKey", env.getProperty("awsSecretAccessKey"));

        dataSet.write()
                .format("com.databricks.spark.redshift")
                .option("url", env.getProperty("output.url"))
                .option("dbtable", destination)
                .option("tempdir", env.getProperty("output.tempDir"))
                .mode(SaveMode.Append)
                .save();

        return true;
    }

}
