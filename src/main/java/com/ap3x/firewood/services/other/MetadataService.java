package com.ap3x.firewood.services.other;

import com.ap3x.firewood.models.FileMetadata;

import java.util.List;
import java.util.Map;

public interface MetadataService {

    List<FileMetadata> getWaitingFilesMetadata();
    Map<String, List<FileMetadata>> getWaitingFilesMetadataByEntity();
    Boolean setMetadataStatusToRunning(final FileMetadata metadata);
    Boolean setMetadataPostProcessingStatus(final FileMetadata metadata);

}
