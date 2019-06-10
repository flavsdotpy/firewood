package com.ap3x.firewood.actors.transformers;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.core.env.Environment;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TypeValidationAndMetadataTransformerTest {

    @Mock
    private MetadataTransformer metadataTransformer;

    @Mock
    private TypeValidationTransformer typeValidationTransformer;

    @InjectMocks
    private TypeValidationAndMetadataTransformer typeValidationAndMetadataTransformer;

    @Mock
    private Environment env;

    @InjectMocks
    private FirewoodSpark firewoodSpark;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        when(env.getProperty("spark.master")).thenReturn("local[*]");
        when(env.getProperty("spark.application")).thenReturn("TransformerTest");
    }

    @Test
    public void transformTest() {
        Dataset<Row> dataset = getDataset();
        Date date = new Date();
        StructType schema = new StructType(
                                new StructField[] {
                                    DataTypes.createStructField(
                                            "test",
                                            DataTypes.StringType,
                                            true
                                    )
                                }
        );

        when(metadataTransformer
                .transform(firewoodSpark, dataset, "path", null, date, date)
        ).thenReturn(dataset);
        when(typeValidationTransformer
                .transform(firewoodSpark, dataset, null, schema, null, null)
        ).thenReturn(dataset);

        assertEquals(
                dataset,
                typeValidationAndMetadataTransformer.transform(firewoodSpark, dataset, "path", schema, date, date)
        );

        verify(metadataTransformer).transform(firewoodSpark, dataset, "path", null, date, date);
        verify(typeValidationTransformer).transform(firewoodSpark, dataset, null, schema, null, null);
    }

    private Dataset<Row> getDataset() {
        List<String> data = new ArrayList<>();
        data.add("dev, engg, 10000");
        data.add("karthik, engg, 20000");
        return firewoodSpark.getSparkSession().sqlContext().createDataset(data, Encoders.STRING()).toDF();
    }

}