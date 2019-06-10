package com.ap3x.firewood.actors;

import com.ap3x.firewood.common.FirewoodContext;
import com.ap3x.firewood.common.FirewoodSpark;
import com.ap3x.firewood.exceptions.InvalidParamsException;
import br.com.rivendel.bigdata.services.dbsource.*;
import com.ap3x.firewood.services.dbsource.DBSourceService;
import com.ap3x.firewood.services.dbsource.MySQLService;
import com.ap3x.firewood.services.filesource.FileSourceService;
import com.ap3x.firewood.services.filesource.ParquetService;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BatchReaderTest {

    @Mock
    private Environment env;

    @Mock
    private FirewoodContext faio;

    @InjectMocks
    private BatchReader batchReader;

    @InjectMocks
    private FirewoodSpark firewoodSpark;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);

        when(env.getProperty("spark.master")).thenReturn("local[*]");
        when(env.getProperty("spark.application")).thenReturn("BatchReaderTest");
    }

    @Test(expected = InvalidParamsException.class)
    public void readThrowsExceptionTest(){
        when(env.getProperty("input.type")).thenReturn("exception");
        batchReader.read("exception");

        verify(env).getProperty("input.type");
    }

    @Test
    public void readDBSourceTest(){
        MySQLService mySQLService = Mockito.mock(MySQLService.class);
        Dataset<Row> testDataset = getDataset();

        when(env.getProperty("input.type")).thenReturn("db");
        when(env.getProperty("input.engine")).thenReturn("mysql");
        when(env.getProperty("input.location")).thenReturn("table");
        when(faio.getBean("mysqlService", DBSourceService.class)).thenReturn(mySQLService);
        when(mySQLService.read("read", "table")).thenReturn(testDataset);

        Dataset<Row> returnedDataset = batchReader.read("read");

        assertEquals(2L, returnedDataset.count());

        verify(env).getProperty("input.type");
        verify(env).getProperty("input.engine");
        verify(env).getProperty("input.location");
        verify(faio).getBean("mysqlService", DBSourceService.class);
        verify(mySQLService).read("read", "table");
    }

    @Test
    public void readFileSourceTest(){
        ParquetService parquetService = Mockito.mock(ParquetService.class);
        Dataset<Row> testDataset = getDataset();

        when(env.getProperty("input.type")).thenReturn("file");
        when(env.getProperty("input.format")).thenReturn("parquet");
        when(faio.getBean("parquetService", FileSourceService.class)).thenReturn(parquetService);
        when(parquetService.read("read", "foo", "bar")).thenReturn(testDataset);

        Dataset<Row> returnedDataset = batchReader.read("read,foo,bar");

        assertEquals(2L, returnedDataset.count());

        verify(env).getProperty("input.type");
        verify(env).getProperty("input.format");
        verify(faio).getBean("parquetService", FileSourceService.class);
        verify(parquetService).read("read", "foo", "bar");
    }

    private Dataset<Row> getDataset(){
        List<String> data = new ArrayList<>();
        data.add("dev, engg, 10000");
        data.add("karthik, engg, 20000");
        return firewoodSpark.getSparkSession().sqlContext().createDataset(data, Encoders.STRING()).toDF();
    }
}