package com.ap3x.firewood.common;

import com.ap3x.firewood.services.other.S3Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.core.io.support.ResourcePropertySource;

import java.io.IOException;

public class FirewoodStarter {

    private static final Log LOGGER = LogFactory.getLog(FirewoodStarter.class);

    private static FirewoodContext context;

    public static FirewoodContext startFaio() {
        LOGGER.info("startFaio() - Starting FAIO with default configuration!");
        setContext();
        addPropertyFile("faio.properties");
        startSpark();
        return context;
    }

    public static FirewoodContext startFaio(final String propertyFile) {
        LOGGER.info("startFaio() - Starting FAIO with custom configuration!");
        setContext();
        addPropertyFile(propertyFile);
        startSpark();
        return context;
    }

    public static FirewoodContext startFaio(final Class clazz, final String propertyFile) {
        LOGGER.info("startFaio() - Starting FAIO with custom configuration!");
        setContext(clazz);
        addPropertyFile(propertyFile);
        startSpark();
        return context;
    }

    public static FirewoodContext startFaioWithExternalProperties(final String bucket, final String key) {
        LOGGER.info("startFaio() - Starting FAIO with default configuration!");
        setContext();
        addExternalPropertyFile(bucket, key);
        startSpark();
        return context;
    }

    public static void addExternalPropertyFile(final String bucket, final String key) {
        LOGGER.info("addExternalPropertyFile() - Adding a new property source");
        setContext();
        try {
            S3Service service = context.getBean(S3Service.class);
            String internalPath = service.downloadFromS3(bucket, key);
            context.getEnvironment().getPropertySources().addLast(new ResourcePropertySource("file:" + internalPath));
        } catch (Exception e) {
            LOGGER.error("addExternalPropertyFile() - Something happened: " + e.getMessage(), e);
        }
    }

    private static void setContext() {
        if (context == null) {
            LOGGER.info("setContext() - Setting a new FirewoodContext");
            context = new FirewoodContext(FirewoodConfiguration.class);
        }
    }

    private static void setContext(final Class clazz) {
        if (context == null) {
            LOGGER.info("setContext() - Setting a new FirewoodContext");
            context = new FirewoodContext(FirewoodConfiguration.class, clazz);
        }
    }

    private static void addPropertyFile(final String propertyFile) {
        try {
            LOGGER.info("addPropertyFile() - Adding a new property source");
            context.getEnvironment().getPropertySources().addLast(new ResourcePropertySource("classpath:" + propertyFile));
        } catch (IOException e) {
            LOGGER.error("addPropertyFile() - Something happened: " + e.getMessage(), e);
        }
    }

    private static void startSpark() {
        LOGGER.info("startSpark() - Starting Spark");
        context.getFaioSpark().setSparkSession();
    }

}
