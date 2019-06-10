package com.ap3x.firewood.helpers;

import com.ap3x.firewood.exceptions.InvalidParamsException;
import com.ap3x.firewood.services.filesource.JsonService;
import com.ap3x.firewood.services.other.S3Service;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

import static com.ap3x.firewood.common.FirewoodConstants.DATA_TYPE_MAP;

@Component
public class FileHelper {

    private static final Log LOGGER = LogFactory.getLog(FileHelper.class);

    @Autowired
    private Environment env;

    @Autowired
    private S3Service s3Service;

    @Autowired
    private JsonService jsonService;

    public String buildOutputDir(final String engine,
                                 final String source,
                                 final String entity,
                                 final String... partitions) {
        LOGGER.debug("buildOutputDir() - Building output dir");

        if (engine == null || source == null || entity == null)
            throw new InvalidParamsException("Missing output dir args");

        final String outputDirBucket = env.getProperty("output.bucket");
        final String outputDirProtocol = env.getProperty("output.protocol");
        String outputDir = String.format("%s://", outputDirProtocol);

        outputDir = outputDir.concat(outputDirBucket + "/");
        outputDir = outputDir.concat(engine + "/");
        outputDir = outputDir.concat(source + "/");
        outputDir = outputDir.concat(entity + "/");

        if (partitions != null) {
            for (int i = 0; i < partitions.length; i++) {
                outputDir = outputDir.concat(partitions[i] + "/");
            }
        }

        LOGGER.debug("buildOutputDir() - Output dir built: " + outputDir);

        return outputDir.substring(0, outputDir.length() - 1);
    }

    public String buildOutputDirByFilePath(final String filePath, final String... partitions) {
        final String[] splitFilePath = filePath.split("/");
        return buildOutputDir(
                getEngineByFilePath(filePath),
                splitFilePath[splitFilePath.length - 3],
                splitFilePath[splitFilePath.length - 2],
                partitions
        );
    }

    public List<String> getOlapEntitiesFromFile(){
        return s3Service.readFromS3ToStringArray(
                env.getProperty("utils.bucket"),
                env.getProperty("olapEntitiesList.path")
        );
    }

    public StructType getSchemaFromJsonFile(final String schemaPath) {
        List<StructField> fields = new ArrayList<>();
        jsonService.read(schemaPath).collectAsList().forEach(row ->
                fields.add(
                        DataTypes.createStructField(
                                row.getAs("name"),
                                DATA_TYPE_MAP.get(row.getAs("type")),
                                row.getAs("nullable")
                        )
                )
        );
        return DataTypes.createStructType(fields);
    }

    public StructType getDMSSchemaByFilePath(final String filePath) {
        final String[] splitFilePath = filePath.split("/");
        String schemaPath = "s3://"
                .concat(env.getProperty("utils.bucket"))
                .concat("/SCHEMAS/")
                .concat(getEngineByFilePath(filePath))
                .concat("/")
                .concat(splitFilePath[splitFilePath.length - 3])
                .concat("/")
                .concat(splitFilePath[splitFilePath.length - 2])
                .concat(".schema.json");
        return getSchemaFromJsonFile(schemaPath);
    }

    public String getEngineByFilePath(final String filePath) {
        if (filePath.toUpperCase().contains("ORACLE/"))
            return "ORACLE";
        if (filePath.toUpperCase().contains("MYSQL/"))
            return "MYSQL";
        if (filePath.toUpperCase().contains("PGSQL/"))
            return "PGSQL";
        if (filePath.toUpperCase().contains("MSSQL/"))
            return "MSSQL";
        if (filePath.toUpperCase().contains("APIS/"))
            return "APIS";
        if (filePath.toUpperCase().contains("GOOGLE_ANALYTICS/"))
            return "GOOGLE_ANALYTICS";
        return "OTHER";
    }

}
