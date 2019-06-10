package com.ap3x.firewood.samples;

import com.ap3x.firewood.actors.BatchReader;
import com.ap3x.firewood.actors.Writer;
import com.ap3x.firewood.actors.transformers.TypeValidationAndMetadataTransformer;
import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.common.FirewoodStarter;
import com.ap3x.firewood.helpers.FileHelper;
import com.ap3x.firewood.helpers.MetadataHelper;
import com.ap3x.firewood.models.FileMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class ProcessWaitingFilesWithEnv {

    static final Log LOGGER = LogFactory.getLog(ProcessWaitingFilesWithEnv.class);

    public static void execute(String[] args) {
        LOGGER.info("main() - Starting file consumption");

        final String env = args[0];
        FirewoodContext firewoodContext = FirewoodStarter.startFaio("faio-" + env + ".properties");

        BatchReader batchReader = firewoodContext.getFaioBatchReader();
        Writer writer = firewoodContext.getFaioWriter();
        MetadataHelper metadataHelper = firewoodContext.getMetadataHelper();
        FileHelper fileHelper = firewoodContext.getFileHelper();

        TypeValidationAndMetadataTransformer transformer = firewoodContext
                                                            .getBean(TypeValidationAndMetadataTransformer.class);

        final List<FileMetadata> files = metadataHelper.getWaitingFiles();

        LOGGER.debug("main() - Updating all found files to RUNNING!");
        for (FileMetadata file: files) {
            file.setExecutionTime(new Date().getTime());
            metadataHelper.setRunning(file);
        }

        for (FileMetadata file: files) {
            LOGGER.info("main() - Consuming file: " + file.getFilePath());

            String filePath = file.getFilePath();
            Date exportedAt = new Date(file.getFileExportTimestamp());
            Date curDate = new Date();



            LOGGER.debug("main() - Consuming: ".concat(filePath));
            try {
                final String outputDir = fileHelper.buildOutputDirByFilePath(
                        filePath,
                        "year=".concat(new SimpleDateFormat("yyyy").format(exportedAt))
                );

                StructType schema = fileHelper.getDMSSchemaByFilePath(filePath);
                Dataset<Row> df = batchReader.read(filePath);
                Dataset<Row> transformedDf = transformer
                        .transform(firewoodContext.getFaioSpark(), df, filePath, schema, exportedAt, curDate);

                writer.write(transformedDf, outputDir, filePath, exportedAt.getTime());
            } catch (Exception e) {
                LOGGER.error("main() - Something happened: ".concat(e.getMessage()), e);
                final FileMetadata metadata = new FileMetadata(
                        curDate.getTime(),
                        false,
                        filePath,
                        exportedAt.getTime(),
                        0L,
                        0L,
                        e.getMessage(),
                        null);
                metadataHelper.setPostProcessingStatus(metadata);
            }
        }
    }
}
