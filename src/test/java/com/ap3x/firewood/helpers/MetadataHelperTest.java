package com.ap3x.firewood.helpers;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.models.FileMetadata;
import com.ap3x.firewood.services.other.DynamoDBService;
import com.ap3x.firewood.services.other.MetadataService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetadataHelperTest {

    @Mock
    private Environment env;

    @Mock
    private FirewoodContext faio;

    @InjectMocks
    private MetadataHelper metadataHelper;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void getWaitingFilesTest(){
        DynamoDBService dynamoDBService = Mockito.mock(DynamoDBService.class);
        FileMetadata metadata = new FileMetadata();

        when(env.getProperty("metadata.engine")).thenReturn("dynamo");
        when(faio.getBean("dynamoService", MetadataService.class)).thenReturn(dynamoDBService);
        when(dynamoDBService.getWaitingFilesMetadata()).thenReturn(Arrays.asList(metadata));

        assertEquals(1, metadataHelper.getWaitingFiles().size());

        verify(env).getProperty("metadata.engine");
        verify(faio).getBean("dynamoService", MetadataService.class);
        verify(dynamoDBService).getWaitingFilesMetadata();
    }

    @Test
    public void getWaitingFilesByEntityTest(){
        DynamoDBService dynamoDBService = Mockito.mock(DynamoDBService.class);
        Map<String, List<FileMetadata>> map = new HashMap<>();
        FileMetadata metadata = new FileMetadata();
        map.put("test", Arrays.asList(metadata));

        when(env.getProperty("metadata.engine")).thenReturn("dynamo");
        when(faio.getBean("dynamoService", MetadataService.class)).thenReturn(dynamoDBService);
        when(dynamoDBService.getWaitingFilesMetadataByEntity()).thenReturn(map);

        assertEquals(1, metadataHelper.getWaitingFilesByEntity().get("test").size());

        verify(env).getProperty("metadata.engine");
        verify(faio).getBean("dynamoService", MetadataService.class);
        verify(dynamoDBService).getWaitingFilesMetadataByEntity();
    }

    @Test
    public void setRunningTest(){
        DynamoDBService dynamoDBService = Mockito.mock(DynamoDBService.class);
        FileMetadata metadata = new FileMetadata();

        when(env.getProperty("metadata.engine")).thenReturn("dynamo");
        when(faio.getBean("dynamoService", MetadataService.class)).thenReturn(dynamoDBService);
        when(dynamoDBService.setMetadataStatusToRunning(metadata)).thenReturn(true);

        assertTrue(metadataHelper.setRunning(metadata));

        verify(env).getProperty("metadata.engine");
        verify(faio).getBean("dynamoService", MetadataService.class);
        verify(dynamoDBService).setMetadataStatusToRunning(metadata);
    }

    @Test
    public void setPostProcessingStatusTest(){
        DynamoDBService dynamoDBService = Mockito.mock(DynamoDBService.class);
        FileMetadata metadata = new FileMetadata();

        when(env.getProperty("metadata.engine")).thenReturn("dynamo");
        when(faio.getBean("dynamoService", MetadataService.class)).thenReturn(dynamoDBService);
        when(dynamoDBService.setMetadataPostProcessingStatus(metadata)).thenReturn(true);

        assertTrue(metadataHelper.setPostProcessingStatus(metadata));

        verify(env).getProperty("metadata.engine");
        verify(faio).getBean("dynamoService", MetadataService.class);
        verify(dynamoDBService).setMetadataPostProcessingStatus(metadata);
    }
}