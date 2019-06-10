package com.ap3x.firewood.helpers;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.models.DBOffset;
import com.ap3x.firewood.services.other.DynamoDBService;
import com.ap3x.firewood.services.other.OffsetService;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class OffsetHelperTest {

    @Mock
    private FirewoodContext faio;

    @Mock
    private Environment env;

    @InjectMocks
    private OffsetHelper offsetHelper;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void getOffsetTest(){
        DynamoDBService dynamoDBService = Mockito.mock(DynamoDBService.class);
        DBOffset offset = new DBOffset();

        when(env.getProperty("offset.engine")).thenReturn("dynamo");
        when(faio.getBean("dynamoService", OffsetService.class)).thenReturn(dynamoDBService);
        when(dynamoDBService.getOffset("table")).thenReturn(offset);

        assertEquals(offset, offsetHelper.getOffset("table"));

        verify(env).getProperty("offset.engine");
        verify(faio).getBean("dynamoService", OffsetService.class);
        verify(dynamoDBService).getOffset("table");
    }

    @Test
    public void updateOffsetTest(){
        DynamoDBService dynamoDBService = Mockito.mock(DynamoDBService.class);
        DBOffset offset = new DBOffset();

        when(env.getProperty("offset.engine")).thenReturn("dynamo");
        when(faio.getBean("dynamoService", OffsetService.class)).thenReturn(dynamoDBService);
        when(dynamoDBService.updateOffset(offset)).thenReturn(true);

        assertTrue(offsetHelper.updateOffset(offset));

        verify(env).getProperty("offset.engine");
        verify(faio).getBean("dynamoService", OffsetService.class);
        verify(dynamoDBService).updateOffset(offset);
    }

}