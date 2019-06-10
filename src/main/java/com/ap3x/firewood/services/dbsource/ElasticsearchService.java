package com.ap3x.firewood.services.dbsource;

import com.ap3x.firewood.common.FirewoodSpark;
import com.ap3x.firewood.services.filesource.OrcService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.util.Properties;

@Service(value = "esService")
public class ElasticsearchService implements DBSourceService {

    private static final Log LOGGER = LogFactory.getLog(OrcService.class);

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Autowired
    private Environment env;

    @Override
    public Boolean write(final Dataset<Row> inputDf, final String indexAndType){
        LOGGER.debug("write() - Writing data to ElasticSearch: " + indexAndType);

        final Properties props = new Properties();
        props.setProperty("es.index.auto.create", "true");
        props.setProperty("es.nodes", env.getProperty("output.host"));
        props.setProperty("es.port", env.getProperty("output.port"));

        firewoodSpark.getSparkSession().sparkContext().setLocalProperties(props);

        JavaEsSparkSQL.saveToEs(inputDf, indexAndType);
        return true;
    }

    @Override
    public Dataset<Row> read(final String query, final String indexAndType){
        LOGGER.debug("read() - Executing query at ElasticSearch: " + query);

        final Properties props = new Properties();
        props.setProperty("es.index.auto.create", "true");
        props.setProperty("es.nodes", env.getProperty("input.host"));
        props.setProperty("es.port", env.getProperty("input.port"));

        firewoodSpark.getSparkSession().sparkContext().setLocalProperties(props);

        return JavaEsSparkSQL.esDF(firewoodSpark.getSparkSession().sqlContext(), indexAndType, query);
    }
}
