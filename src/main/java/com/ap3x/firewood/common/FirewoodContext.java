package com.ap3x.firewood.common;

import com.ap3x.firewood.actors.BatchReader;
import com.ap3x.firewood.actors.Writer;
import com.ap3x.firewood.helpers.FileHelper;
import com.ap3x.firewood.helpers.MetadataHelper;
import com.ap3x.firewood.helpers.OffsetHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.Environment;

public class FirewoodContext extends AnnotationConfigApplicationContext {

    private static final Log LOGGER = LogFactory.getLog(FirewoodContext.class);

    public FirewoodContext(Class<?>... annotatedClasses) {
        super(annotatedClasses);
    }

    public BatchReader getFaioBatchReader() {
        LOGGER.info("getFaioBatchReader() - Returning BatchReader bean");
        return this.getBean("batchReader", BatchReader.class);
    }

    public Writer getFaioWriter() {
        LOGGER.info("getFaioWriter() - Returning Writer bean");
        return this.getBean("writer", Writer.class);
    }

    public MetadataHelper getMetadataHelper() {
        LOGGER.info("getMetadataHelper() - Returning MetadataHelper bean");
        return this.getBean(MetadataHelper.class);
    }

    public FileHelper getFileHelper() {
        LOGGER.info("getFileHelper() - Returning FileHelper bean");
        return this.getBean(FileHelper.class);
    }

    public OffsetHelper getOffsetHelper() {
        LOGGER.info("getOffsetHelper() - Returning OffsetHelper bean");
        return this.getBean(OffsetHelper.class);
    }

    public FirewoodSpark getFaioSpark(){
        LOGGER.debug("getFaioSpark() - Returning FirewoodSpark bean");
        return this.getBean("sparkSetup", FirewoodSpark.class);
    }

    public String getProperty(final String key){
        LOGGER.debug("getProperty() - Returning property: " + key);
        return this.getBean(Environment.class).getProperty(key);
    }

    public <T>T getProperty(final String key, final Class<T> clazz){
        LOGGER.debug("getProperty() - Returning property: " + key);
        return this.getBean(Environment.class).getProperty(key, clazz);
    }
}
