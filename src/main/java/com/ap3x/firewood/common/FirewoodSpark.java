package com.ap3x.firewood.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

@Component(value = "sparkSetup")
public class FirewoodSpark {

    private static final Log LOGGER = LogFactory.getLog(FirewoodSpark.class);

    private SparkSession sparkSession;

    @Autowired
    private Environment env;

    public SparkSession getSparkSession() {
        LOGGER.debug("getSparkSession() - Getting current session");
        setSparkSession();
        return sparkSession;
    }

    public void setSparkSession() {
        if (this.sparkSession == null)
            LOGGER.info("getSparkSession() - Generating a new spark session");
            this.sparkSession = SparkSession.builder()
                    .master(env.getProperty("spark.master"))
                    .appName(env.getProperty("spark.application"))
                    .getOrCreate();
    }
}
