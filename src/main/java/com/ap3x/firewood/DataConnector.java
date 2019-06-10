package com.ap3x.firewood;

import com.ap3x.firewood.actors.BatchReader;
import com.ap3x.firewood.actors.Writer;
import com.ap3x.firewood.actors.transformers.MetadataTransformer;
import com.ap3x.firewood.actors.transformers.Transformer;
import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.common.FirewoodStarter;
import com.ap3x.firewood.helpers.FileHelper;
import com.ap3x.firewood.helpers.MetadataHelper;
import com.ap3x.firewood.helpers.OffsetHelper;
import com.ap3x.firewood.models.DBOffset;
import com.ap3x.firewood.models.FileMetadata;
import com.ap3x.firewood.services.streamsource.StreamingSourceService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import scala.Tuple2;

import java.util.Date;
import java.util.List;

import static com.ap3x.firewood.common.FirewoodConstants.BASE_QUERY;

public class DataConnector {

    private static final Log LOGGER = LogFactory.getLog(DataConnector.class);

    public static void main(String[] args) {
        LOGGER.info("main() - Starting process...");
        final FirewoodContext faio = FirewoodStarter.startFaioWithExternalProperties(args[0], args[1]);

        final String inputType = faio.getProperty("input.type");

        if (inputType.equals("stream"))
            streamProcess(faio);
        else if (inputType.equals("db"))
            databaseBasedBatchProcess(faio);
        else if (inputType.equals("file"))
            fileBasedBatchProcess(faio);
    }

    private static void streamProcess(final FirewoodContext faio) {
        LOGGER.info("streamProcess() - Process mode: Stream load");
        final MetadataTransformer transformer = faio.getBean(MetadataTransformer.class);
        final FileHelper fileHelper = faio.getFileHelper();
        final String outputType = faio.getProperty("output.type");
        final Writer writer = faio.getFaioWriter();
        final StructType schema = fileHelper.getDMSSchemaByFilePath(faio.getProperty("input.schemaPath"));

        final String streamEngine = faio.getProperty("input.engine");
        if (streamEngine.equals("kafka")) {
            final String topics = faio.getProperty("input.topics");
            final Tuple2<StreamingContext, JavaInputDStream<ConsumerRecord<String, String>>> tuple;
            try {
                tuple = (Tuple2<StreamingContext, JavaInputDStream<ConsumerRecord<String, String>>>) faio
                        .getBean("kafkaService", StreamingSourceService.class).read();

            final StreamingContext streamingContext = tuple._1;
            final JavaInputDStream<ConsumerRecord<String, String>> stream = tuple._2;

            final String outputLocation = faio.getProperty("output.location");
            final String outputDir = fileHelper.buildOutputDir(
                    streamEngine,
                    faio.getProperty("input.source"),
                    topics
            );
            final String output = outputLocation.equals(null) ? outputLocation : outputDir;

            stream.foreachRDD( rdd -> {
                    Date date = new Date();
                    JavaRDD<Row> rows = rdd.map(record ->
                        new GenericRow(record.value().split(faio.getProperty("input.delimiter")))
                    );
                    Dataset<Row> dataset = faio.getFaioSpark().getSparkSession().sqlContext()
                            .createDataFrame(rows, schema);

                    Dataset<Row> transformedDataset = transformer.transform(
                            faio.getFaioSpark(),
                            dataset,
                            topics,
                            null,
                            date,
                            date
                    );
                    writer.write(transformedDataset, output, null, null);
                }
            );
            } catch (ClassNotFoundException e) {
                LOGGER.error("streamProcess() - Something happened: " + e.getMessage(), e);
            }

        } else if (streamEngine.equals("kinesis")){
            throw new RuntimeException("Not yet implemented!");
        }
    }

    private static void databaseBasedBatchProcess(final FirewoodContext faio) {
        final BatchReader reader = faio.getFaioBatchReader();
        final Writer writer = faio.getFaioWriter();
        final OffsetHelper offsetHelper = faio.getOffsetHelper();
        final FileHelper fileHelper = faio.getFileHelper();
        final MetadataTransformer transformer = faio.getBean(MetadataTransformer.class);

        final String inputLocation = faio.getProperty("input.location");
        final String outputType = faio.getProperty("output.type");
        String output = null;
        final DBOffset offset = offsetHelper.getOffset(inputLocation);
        try {
            LOGGER.debug("databaseBasedBatchProcess() - Consuming: " + inputLocation);
            final Date curDate = new Date();

            if (outputType.equals("db"))
                output = faio.getProperty("output.location");
            else if (outputType.equals("file"))
                output = fileHelper.buildOutputDir(
                        faio.getProperty("input.engine"),
                        faio.getProperty("input.source"),
                        inputLocation
                );

            final String query = BASE_QUERY
                    .replace("{table}", inputLocation)
                    .replace("{offset_field}", faio.getProperty("input.offsetField"))
                    .replace("{last_execution}", offset.getLastExecution().toString())
                    .replace("{now}", String.valueOf(curDate.getTime()));

            Dataset<Row> df = reader.read(query);
            Dataset<Row> transformedDf = transformer
                    .transform(faio.getFaioSpark(), df, inputLocation, null, curDate, curDate);
            writer.write(transformedDf, output, inputLocation, curDate.getTime());

        } catch (Exception e) {
            LOGGER.error("databaseBasedBatchProcess() - Something happened: " + e.getMessage(), e);
            offset.setMessage(e.getMessage());
            offsetHelper.updateOffset(offset);
        }

    }

    private static void fileBasedBatchProcess(final FirewoodContext faio) {
        final BatchReader reader = faio.getFaioBatchReader();
        final Writer writer = faio.getFaioWriter();
        final MetadataHelper metadataHelper = faio.getMetadataHelper();
        final FileHelper fileHelper = faio.getFileHelper();
        final Transformer transformer = faio.getBean(faio.getProperty("transformer"), Transformer.class);

        final String outputType = faio.getProperty("output.type");

        final List<FileMetadata> files = metadataHelper.getWaitingFiles();

        for (FileMetadata file : files) {
            file.setExecutionTime(new Date().getTime());
            metadataHelper.setRunning(file);
        }

        for (FileMetadata file : files) {
            final String filePath = file.getFilePath();
            final Date exportedAt = new Date(file.getFileExportTimestamp());
            final Date curDate = new Date();
            String output = null;

            if (outputType.equals("db"))
                output = faio.getProperty("output.location");
            else if (outputType.equals("file"))
                output = fileHelper.buildOutputDirByFilePath(file.getFilePath());

            try {
                LOGGER.debug("fileBasedBatchProcess() - Consuming: " + filePath);
                StructType schema = fileHelper.getDMSSchemaByFilePath(filePath);
                Dataset<Row> df = reader.read(filePath);
                Dataset<Row> transformedDf = transformer
                        .transform(faio.getFaioSpark(), df, filePath, schema, exportedAt, curDate);

                writer.write(transformedDf, output, filePath, exportedAt.getTime());
            } catch (Exception e) {
                LOGGER.error("fileBasedBatchProcess() - Something happened: " + e.getMessage(), e);
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
