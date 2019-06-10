package com.ap3x.firewood.helpers;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.services.other.MetadataService;
import com.ap3x.firewood.models.FileMetadata;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component(value = "metadataHelper")
public class MetadataHelper {

    private static final Log LOGGER = LogFactory.getLog(MetadataHelper.class);

    @Autowired
    private Environment env;

    @Autowired
    private FirewoodContext faio;

    public List<FileMetadata> getWaitingFiles(){
        LOGGER.debug("getWaitingFilesMetadata() - Listing waiting items");

        return faio.getBean(
                env.getProperty("metadata.engine").concat("Service"),
                MetadataService.class
        ).getWaitingFilesMetadata();
    }

    public Map<String, List<FileMetadata>> getWaitingFilesByEntity(){
        LOGGER.debug("getWaitingFilesMetadataByEntity() - Listing waiting items grouped by entity");

        return faio.getBean(
                env.getProperty("metadata.engine").concat("Service"),
                MetadataService.class
        ).getWaitingFilesMetadataByEntity();
    }

    public Boolean setRunning(final FileMetadata metadata) {
        LOGGER.debug("getWaitingFilesMetadataByEntity() - Listing waiting items grouped by entity");

        return faio.getBean(
                env.getProperty("metadata.engine").concat("Service"),
                MetadataService.class
        ).setMetadataStatusToRunning(metadata);
    }

    public Boolean setPostProcessingStatus(final FileMetadata metadata) {
        LOGGER.debug("getWaitingFilesMetadataByEntity() - Listing waiting items grouped by entity");

        return faio.getBean(
                env.getProperty("metadata.engine").concat("Service"),
                MetadataService.class
        ).setMetadataPostProcessingStatus(metadata);
    }

}
