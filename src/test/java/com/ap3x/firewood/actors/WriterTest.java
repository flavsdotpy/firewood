package com.ap3x.firewood.actors;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.common.FirewoodSpark;
import com.ap3x.firewood.exceptions.InvalidParamsException;
import com.ap3x.firewood.helpers.CommonHelper;
import com.ap3x.firewood.helpers.MetadataHelper;
import com.ap3x.firewood.helpers.OffsetHelper;
import com.ap3x.firewood.models.FileMetadata;
import com.ap3x.firewood.models.DBOffset;
import com.ap3x.firewood.services.dbsource.DBSourceService;
import com.ap3x.firewood.services.dbsource.MySQLService;
import com.ap3x.firewood.services.filesource.FileSourceService;
import com.ap3x.firewood.services.filesource.ParquetService;
import com.ap3x.firewood.services.streamsource.KafkaStreamService;
import com.ap3x.firewood.services.streamsource.StreamingSourceService;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class WriterTest {

    @Mock
    private Environment env;

    @Mock
    private FirewoodContext faio;

    @Mock
    private OffsetHelper offsetHelper;

    @Mock
    private MetadataHelper metadataHelper;

    @Mock
    private CommonHelper commonHelper;

    @InjectMocks
    private Writer writer;

    @InjectMocks
    private FirewoodSpark firewoodSpark;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        when(env.getProperty("spark.master")).thenReturn("local[*]");
        when(env.getProperty("spark.application")).thenReturn("WriterTest");
    }

    @Test(expected = InvalidParamsException.class)
    public void writeThrowsExceptionTest(){
        when(env.getProperty("output.type")).thenReturn("exception");

        writer.write(null,"output", "sourceKey", 1L);

        verify(env).getProperty("output.type");
    }

    @Test
    public void writeToDBSourcesOKTest() {
        MySQLService mySQLService = Mockito.mock(MySQLService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("db");
        when(env.getProperty("output.engine")).thenReturn("mysql");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("mysqlService", DBSourceService.class)).thenReturn(mySQLService);
        when(mySQLService.write(dataset, "output")).thenReturn(true);

        assertTrue(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.engine");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("mysqlService", DBSourceService.class);
        verify(mySQLService).write(dataset, "output");
    }

    @Test
    public void writeToDBSourcesCatchTest() {
        MySQLService mySQLService = Mockito.mock(MySQLService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("db");
        when(env.getProperty("output.engine")).thenReturn("mysql");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("mysqlService", DBSourceService.class)).thenReturn(mySQLService);
        when(mySQLService.write(dataset, "output")).thenThrow(new RuntimeException());

        assertFalse(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.engine");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("mysqlService", DBSourceService.class);
        verify(mySQLService).write(dataset, "output");
    }

    @Test
    public void writeToFileSourcesOKTest() {
        ParquetService parquetService = Mockito.mock(ParquetService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("file");
        when(env.getProperty("output.format")).thenReturn("parquet");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("parquetService", FileSourceService.class)).thenReturn(parquetService);
        when(parquetService.write(dataset, "output")).thenReturn(true);

        assertTrue(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.format");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("parquetService", FileSourceService.class);
        verify(parquetService).write(dataset, "output");
    }

    @Test
    public void writeToFileSourcesCatchTest() {
        ParquetService parquetService = Mockito.mock(ParquetService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("file");
        when(env.getProperty("output.format")).thenReturn("parquet");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("parquetService", FileSourceService.class)).thenReturn(parquetService);
        when(parquetService.write(dataset, "output")).thenThrow(new RuntimeException());

        assertFalse(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.format");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("parquetService", FileSourceService.class);
        verify(parquetService).write(dataset, "output");
    }

    @Test
    public void writeToStreamSourcesOKTest() throws ClassNotFoundException {
        KafkaStreamService kafkaStreamService = Mockito.mock(KafkaStreamService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("stream");
        when(env.getProperty("output.engine")).thenReturn("kafka");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("kafkaService", StreamingSourceService.class)).thenReturn(kafkaStreamService);
        when(kafkaStreamService.write(any(JavaRDD.class), eq("output"))).thenReturn(true);

        assertTrue(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.engine");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("kafkaService", StreamingSourceService.class);
        verify(kafkaStreamService).write(any(JavaRDD.class), eq("output"));
    }

    @Test
    public void writeToStreamSourcesCatchTest() throws ClassNotFoundException {
        KafkaStreamService kafkaStreamService = Mockito.mock(KafkaStreamService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("stream");
        when(env.getProperty("output.engine")).thenReturn("kafka");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("kafkaService", StreamingSourceService.class)).thenReturn(kafkaStreamService);
        when(kafkaStreamService.write(any(JavaRDD.class), eq("output"))).thenThrow(new RuntimeException());

        assertFalse(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.engine");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("kafkaService", StreamingSourceService.class);
        verify(kafkaStreamService).write(any(JavaRDD.class), eq("output"));
    }

    @Test
    public void updateMetadataTest() {
        MySQLService mySQLService = Mockito.mock(MySQLService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("db");
        when(env.getProperty("output.engine")).thenReturn("mysql");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(true);
        when(env.getProperty("offset", Boolean.class)).thenReturn(false);
        when(faio.getBean("mysqlService", DBSourceService.class)).thenReturn(mySQLService);
        when(mySQLService.write(dataset, "output")).thenReturn(true);
        when(commonHelper.countLines(dataset)).thenReturn(getCountMap());
        when(metadataHelper.setPostProcessingStatus(any(FileMetadata.class))).thenReturn(true);

        assertTrue(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.engine");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("mysqlService", DBSourceService.class);
        verify(mySQLService).write(dataset, "output");
        verify(commonHelper).countLines(dataset);
        verify(metadataHelper).setPostProcessingStatus(any(FileMetadata.class));
    }

    @Test
    public void updateOffsetTest() {
        MySQLService mySQLService = Mockito.mock(MySQLService.class);
        Dataset<Row> dataset = getDataset();

        when(env.getProperty("output.type")).thenReturn("db");
        when(env.getProperty("output.engine")).thenReturn("mysql");
        when(env.getProperty("metadata", Boolean.class)).thenReturn(false);
        when(env.getProperty("offset", Boolean.class)).thenReturn(true);
        when(faio.getBean("mysqlService", DBSourceService.class)).thenReturn(mySQLService);
        when(mySQLService.write(dataset, "output")).thenReturn(true);
        when(offsetHelper.updateOffset(any(DBOffset.class))).thenReturn(true);

        assertTrue(writer.write(dataset, "output", "sourceKey", 1L));

        verify(env).getProperty("output.type");
        verify(env).getProperty("output.engine");
        verify(env).getProperty("metadata", Boolean.class);
        verify(env).getProperty("offset", Boolean.class);
        verify(faio).getBean("mysqlService", DBSourceService.class);
        verify(mySQLService).write(dataset, "output");
        verify(offsetHelper).updateOffset(any(DBOffset.class));
    }

    private Dataset<Row> getDataset(){
        List<String> data = new ArrayList<>();
        data.add("dev, engg, 10000");
        data.add("karthik, engg, 20000");
        return firewoodSpark.getSparkSession().sqlContext().createDataset(data, Encoders.STRING()).toDF();
    }

    private Map<String, Integer> getCountMap(){
        return ImmutableMap.of(
                "success", 1000,
                "error", 1000
        );
    }
}