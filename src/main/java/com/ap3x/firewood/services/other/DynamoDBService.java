package com.ap3x.firewood.services.other;

import com.ap3x.firewood.exceptions.InvalidParamsException;
import com.ap3x.firewood.models.FileMetadata;
import com.ap3x.firewood.models.DBOffset;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.*;

import static com.ap3x.firewood.common.FirewoodConstants.SOURCE_TRACKING_KEY_MAP;
import static com.ap3x.firewood.common.FirewoodConstants.SOURCE_TYPE_KEY_MAP;

@Service(value = "dynamoService")
public class DynamoDBService implements MetadataService, OffsetService {

    private static final Log LOGGER = LogFactory.getLog(DynamoDBService.class);

    @Autowired
    private Environment env;

    @Autowired
    private S3Service s3Service;

    public Boolean write(final String tableName, final UpdateItemSpec updateItemSpec) {
        final AmazonDynamoDB service = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDB client = new DynamoDB(service);
        final Table table = client.getTable(tableName);
        try {
            table.updateItem(updateItemSpec);
            LOGGER.debug("updateDataAtDynamo() - Data updated successfully");
            return true;
        }
        catch (Exception e) {
            LOGGER.error("updateDataAtDynamo() - Something happened: " + e.getMessage(), e);
            return false;
        }
    }

    public List<Map<String, AttributeValue>> read(final ScanRequest scanRequest) {
        final AmazonDynamoDB service = AmazonDynamoDBClientBuilder.standard().build();
        Map<String, AttributeValue> lastKeyEvaluated = null;
        List<Map<String, AttributeValue>> values = new ArrayList<>();

        try {
            do {
                ScanResult scanResult = service.scan(scanRequest.withExclusiveStartKey(lastKeyEvaluated));
                values.addAll(scanResult.getItems());
                lastKeyEvaluated = scanResult.getLastEvaluatedKey();
            } while(lastKeyEvaluated != null);
        }
        catch (Exception e) {
            LOGGER.error("getWaitingFilesMetadata() - Something happened: " + e.getMessage(), e);
        }

        return values;
    }

    @Override
    public List<FileMetadata> getWaitingFilesMetadata() {
        List<FileMetadata> items = new ArrayList<>();

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":status", new AttributeValue().withS("WAITING"));

        this.read(new ScanRequest()
                .withIndexName(env.getProperty("metadata.index"))
                .withTableName(env.getProperty("metadata.table"))
                .withFilterExpression("file_status = :status")
                .withExpressionAttributeValues(expressionAttributeValues)
        ).forEach(value -> items.add(
                new FileMetadata(
                        value.get(getPrimaryKey()).getS(),
                        Long.parseLong(value.get(getRangeKey()).getN())
                )
        ));

        return items;
    }

    @Override
    public Map<String, List<FileMetadata>> getWaitingFilesMetadataByEntity() {
        final Map<String, List<FileMetadata>> segregatedLists = new HashMap<>();
        final List<FileMetadata> items = getWaitingFilesMetadata();
        final List<String> entities = s3Service.readFromS3ToStringArray(
                env.getProperty("utils.bucket"),
                env.getProperty("entityList.path")
        );
        entities.forEach(entity ->
            items.forEach(item -> {
                if (item.getFilePath().contains(entity))
                    if (segregatedLists.containsKey(entity))
                        segregatedLists.get(entity).add(item);
                    else
                        segregatedLists.put(entity, Arrays.asList(item));
            })
        );
        return segregatedLists;
    }


    @Override
    public Boolean setMetadataStatusToRunning(final FileMetadata metadata) {
        final AmazonDynamoDB service = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDB client = new DynamoDB(service);
        final Table table = client.getTable(env.getProperty("metadata.table"));
        try {
            table.updateItem(getRunningItemSpec(metadata));
            LOGGER.debug("setMetadataStatusToRunning() - FileMetadata updated successfully for: " + metadata.getFilePath());
            return true;
        }
        catch (Exception e) {
            LOGGER.error("setMetadataStatusToRunning() - Something happened: " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public Boolean setMetadataPostProcessingStatus(final FileMetadata metadata) {
        final AmazonDynamoDB service = AmazonDynamoDBClientBuilder.standard().build();
        final DynamoDB client = new DynamoDB(service);
        final Table table = client.getTable(env.getProperty("metadata.table"));
        try {
            table.updateItem(getPostProcessingItemSpec(metadata));
            LOGGER.debug("setMetadataPostProcessingStatus() - FileMetadata updated successfully for: " + metadata.getFilePath());
            return true;
        }
        catch (Exception e) {
            LOGGER.error("setMetadataPostProcessingStatus() - Something happened: " + e.getMessage(), e);
            return false;
        }
    }

    @Override
    public DBOffset getOffset(final String outputTable){
        LOGGER.debug("getOffset() - Looking for offset at DynamoDB");

        final Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":outputTable", new AttributeValue().withS(outputTable));

        final List<Map<String, AttributeValue>> result = this.read(new ScanRequest()
                .withTableName(env.getProperty("offset.table"))
                .withFilterExpression("output_table = :outputTable")
                .withExpressionAttributeValues(expressionAttributeValues)
        );

        if (!result.isEmpty() && result.size() == 1) {
            LOGGER.debug("getOffset() - Found a result");
            Map<String, AttributeValue> record = result.get(0);
            final DBOffset offset = new DBOffset(
                    Long.parseLong(record.get("last_execution").getN()),
                    record.get("source_key").getS(),
                    record.get("message").getS()
            );
            return offset;
        }
        throw new InvalidParamsException("Output not found with this name at DynamoDB, or found with duplicity");
    }

    @Override
    public Boolean updateOffset(final DBOffset offset) {
        LOGGER.debug("getOffset() - Updating offset at DynamoDB");
        return this.write(
                env.getProperty("offset.table"),
                new UpdateItemSpec()
                        .withPrimaryKey("source_key", offset.getSourceKey())
                        .withUpdateExpression("set last_execution = :lastExecution, message = :message")
                        .withValueMap(new ValueMap()
                                .withNumber(":lastExecution", offset.getLastExecution())
                                .withString(":message", offset.getMessage())
                        )
                        .withReturnValues(ReturnValue.ALL_NEW)
        );
    }

    private UpdateItemSpec getRunningItemSpec(final FileMetadata metadata) {
        LOGGER.debug("getRunningItemSpec() - Generating item spec");

        return new UpdateItemSpec()
                    .withPrimaryKey(getPrimaryKey(), metadata.getFilePath(), getRangeKey(), metadata.getFileExportTimestamp())
                    .withUpdateExpression("set updated_at = :update, file_status = :fileStatus")
                    .withValueMap(new ValueMap()
                            .withNumber(":update", metadata.getExecutionTime())
                            .withString(":fileStatus", "RUNNING")
                    )
                    .withReturnValues(ReturnValue.ALL_NEW);
    }

    private UpdateItemSpec getPostProcessingItemSpec(final FileMetadata metadata) {
        LOGGER.debug("getPostProcessingItemSpec() - Generating item spec");

        String status = "SUCCESS";
        if (!metadata.getResult())
            status = "FAILED";

        return new UpdateItemSpec()
                .withPrimaryKey(getPrimaryKey(), metadata.getFilePath(), getRangeKey(), metadata.getFileExportTimestamp())
                .withUpdateExpression(getPostProcessingUpdateExpression(metadata))
                .withValueMap(getPostProcessingValueMap(metadata, status))
                .withReturnValues(ReturnValue.ALL_NEW);
    }


    private String getPostProcessingUpdateExpression(final FileMetadata metadata) {
        String updateExpression = "set updated_at = :update, total_lines = :totalLines, total_success_lines = :totalSuccessLines, " +
                "executed_at = :executedAt, ";
        if (metadata.getExceptionMessage() != null)
            updateExpression = updateExpression.concat("exception_message = :exceptionMsg, ");
        if (metadata.getOutputRef() != null)
            updateExpression = updateExpression.concat("output_dir = :outputDir, ");
        return updateExpression.concat("file_status = :fileStatus");
    }

    private ValueMap getPostProcessingValueMap(final FileMetadata metadata, final String status) {
        ValueMap valueMap = new ValueMap()
                .withNumber(":update", metadata.getExecutionTime())
                .withNumber(":totalLines", metadata.getTotalLines())
                .withNumber(":totalSuccessLines", metadata.getSuccessLines())
                .withNumber(":executedAt", metadata.getExecutionTime())
                .withString(":fileStatus", status);
        if (metadata.getExceptionMessage() != null)
            valueMap = valueMap.withString(":exceptionMsg", metadata.getExceptionMessage());
        if (metadata.getOutputRef() != null)
            valueMap = valueMap.withString(":outputDir", metadata.getOutputRef());
        return valueMap;
    }

    private String getPrimaryKey() {
        return SOURCE_TYPE_KEY_MAP.get(env.getProperty("input.type"));
    }

    private String getRangeKey() {
        return SOURCE_TRACKING_KEY_MAP.get(env.getProperty("input.type"));
    }



}
