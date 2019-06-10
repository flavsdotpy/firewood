package com.ap3x.firewood.actors.transformers;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.Before;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.when;

public class TypeValidationTransformerTest {

    @InjectMocks
    private TypeValidationTransformer transformer;

    @Mock
    private Environment env;

    @InjectMocks
    private FirewoodSpark firewoodSpark;

    @Before
    public void setUp(){
        MockitoAnnotations.initMocks(this);

        when(env.getProperty("spark.master")).thenReturn("local[*]");
        when(env.getProperty("spark.application")).thenReturn("TransformerTest");
    }

    private Dataset<Row> getDataset(){
        List<String> data = new ArrayList<>();
        data.add("dev, engg, 10000");
        data.add("karthik, engg, 20000");
        return firewoodSpark.getSparkSession().sqlContext().createDataset(data, Encoders.STRING()).toDF();
    }
}