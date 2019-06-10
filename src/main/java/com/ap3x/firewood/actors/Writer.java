package com.ap3x.firewood.actors;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.exceptions.InvalidParamsException;
import com.ap3x.firewood.helpers.CommonHelper;
import com.ap3x.firewood.helpers.MetadataHelper;
import com.ap3x.firewood.helpers.OffsetHelper;
import com.ap3x.firewood.models.DBOffset;
import com.ap3x.firewood.models.FileMetadata;
import com.ap3x.firewood.services.dbsource.DBSourceService;
import com.ap3x.firewood.services.filesource.FileSourceService;
import com.ap3x.firewood.services.streamsource.StreamingSourceService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Map;

@Component(value = "writer")
public class Writer {

    private static final Log LOGGER = LogFactory.getLog(Writer.class);

    @Autowired
    private FirewoodContext faio;

    @Autowired
    private Environment env;

    @Autowired
    private MetadataHelper metadataHelper;

    @Autowired
    private CommonHelper commonHelper;

    @Autowired
    private OffsetHelper offsetHelper;

    public Boolean write(final Dataset<Row> dataset, final String output,
                         final String sourceKey, final Long sourceTracking) {
        final String writeSourceType = env.getProperty("output.type");

        if (writeSourceType.equals("db"))
            return writeToDBSources(dataset, output, sourceKey, sourceTracking);
        if (writeSourceType.equals("file"))
            return writeToFileSources(dataset, output, sourceKey, sourceTracking);
        if (writeSourceType.equals("stream"))
            return writeToStreamSources(dataset, output, sourceKey, sourceTracking);

        throw new InvalidParamsException("Resource type not set or invalid: " + writeSourceType);
    }

    private Boolean writeToDBSources(final Dataset<Row> dataset, final String output, final String sourceKey,
                                     final Long sourceTracking) {
        LOGGER.debug("writeToDBSources() - Writing to DB sources");
        Boolean result = false;
        String exceptionMessage = null;

        try {
            LOGGER.debug("writeToDBSources() - Writing to: " + output);
            faio.getBean(
                    env.getProperty("output.engine").concat("Service"),
                    DBSourceService.class
            ).write(dataset, output);
            result = true;
        } catch (Exception e){
            result = false;
            exceptionMessage = e.getMessage();
            LOGGER.error("writeToDBSources() - Something happened: " + e.getMessage(), e);
        } finally {
            updateMetadata(dataset, sourceKey, sourceTracking, result, exceptionMessage, output);
            updateOffset(sourceKey, sourceTracking, result, exceptionMessage);
        }
        return result;
    }

    private Boolean writeToFileSources(final Dataset<Row> dataset, final String output, final String sourceKey,
                                       final Long sourceTracking) {
        LOGGER.debug("write() - Writing to file sources");
        Boolean result = false;
        String exceptionMessage = null;

        try {
            LOGGER.debug("writeToDBSources() - Writing to: " + output);
            faio.getBean(
                    env.getProperty("output.format").concat("Service"),
                    FileSourceService.class
            ).write(dataset, output);
            result = true;
        } catch (Exception e){
            result = false;
            exceptionMessage = e.getMessage();
            LOGGER.error("writeToDBSources() - Something happened: " + e.getMessage(), e);
        } finally {
            updateMetadata(dataset, sourceKey, sourceTracking, result, exceptionMessage, output);
            updateOffset(sourceKey, sourceTracking, result, exceptionMessage);
        }
        return result;
    }

    private Boolean writeToStreamSources(final Dataset<Row> dataset, final String output, final String sourceKey,
                                         final Long sourceTracking) {
        LOGGER.debug("write() - Writing to stream sources");
        Boolean result = false;
        String exceptionMessage = null;

        try {
            LOGGER.debug("writeToDBSources() - Writing to: " + output);
            faio.getBean(
                    env.getProperty("output.engine").concat("Service"),
                    StreamingSourceService.class
            ).write(dataset.javaRDD(), output);
            result = true;
        } catch (Exception e){
            result = false;
            exceptionMessage = e.getMessage();
            LOGGER.error("writeToDBSources() - Something happened: " + e.getMessage(), e);
        } finally {
            updateMetadata(dataset, sourceKey, sourceTracking, result, exceptionMessage, output);
            updateOffset(sourceKey, sourceTracking, result, exceptionMessage);
        }
        return result;
    }

    private void updateMetadata(Dataset<Row> dataset,
                                String sourceKey,
                                Long sourceTracking,
                                Boolean result,
                                String exceptionMessage,
                                String outputRef) {
        Long totalSuccessLines = 0L;
        Long totalLines = 0L;
        final Boolean hasMetadataTracking = env.getProperty("metadata", Boolean.class);

        if (hasMetadataTracking) {
            LOGGER.debug("updateMetadata() - Updating metadata...");
            final Long executionTime = new Date().getTime();

            if (result) {
                LOGGER.debug("updateMetadata() - Counting lines...");
                Map<String, Integer> counts = commonHelper.countLines(dataset);
                totalSuccessLines = counts.containsKey("success") ? counts.get("success").longValue() : 0;
                totalLines = counts.containsKey("error") ? counts.get("error") + totalSuccessLines : totalSuccessLines;
            }

            final FileMetadata metadata = new FileMetadata(
                    executionTime,
                    result,
                    sourceKey,
                    sourceTracking,
                    totalLines,
                    totalSuccessLines,
                    exceptionMessage,
                    outputRef
            );
            metadataHelper.setPostProcessingStatus(metadata);
        }
    }

    private void updateOffset(final String sourceKey, final Long sourceTracking, final Boolean result,
                              final String exceptionMessage) {
        final Boolean hasOffsetTracking = env.getProperty("offset", Boolean.class);
        if (hasOffsetTracking && result) {
            LOGGER.debug("updateOffset() - Updating offset...");
            final DBOffset offset = new DBOffset(sourceTracking, sourceKey, exceptionMessage);
            if (exceptionMessage == null)
                offset.setMessage("Success");
            offsetHelper.updateOffset(offset);
        }
    }
}
