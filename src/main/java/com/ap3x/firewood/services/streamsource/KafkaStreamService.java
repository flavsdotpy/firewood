package com.ap3x.firewood.services.streamsource;

import com.ap3x.firewood.common.FirewoodSpark;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.ConsumerStrategy;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.ap3x.firewood.common.FirewoodConstants.COMMON_DEMILITER;

@Service(value = "kafkaService")
public class KafkaStreamService implements StreamingSourceService {

    @Autowired
    private Environment env;

    @Autowired
    private FirewoodSpark firewoodSpark;


    @Override
    public Tuple2 read() throws ClassNotFoundException {
        Class keyDeserializer = Class.forName(env.getProperty("input.keyDeserializer"));
        Class valueDeserializer = Class.forName(env.getProperty("input.valueDeserializer"));
        return getKafkaConsumerStreamingContext(keyDeserializer, valueDeserializer);
    }

    @Override
    public Boolean write(final JavaRDD<Row> rdd, final String topic) throws ClassNotFoundException {
        Class keyDeserializer = Class.forName(env.getProperty("input.keyDeserializer"));
        Class valueDeserializer = Class.forName(env.getProperty("input.valueDeserializer"));
        return sendRddToKafka(rdd, topic, keyDeserializer, valueDeserializer);
    }

    private Tuple2<StreamingContext, JavaInputDStream<ConsumerRecord<String, String>>> getKafkaConsumerStreamingContext(
            final Class keyDeserializer,
            final Class valueDeserializer) {

        final SparkContext sparkContext = firewoodSpark.getSparkSession().sparkContext();
        final Duration kinesisCheckpointInterval = Seconds.apply(2);
        final JavaStreamingContext streamingContext = new JavaStreamingContext(new JavaSparkContext(sparkContext), kinesisCheckpointInterval);
        final List<String> topics = Arrays.asList(env.getProperty("input.topics").split(COMMON_DEMILITER));

        final ConsumerStrategy consumerStrategy = ConsumerStrategies.Subscribe(
                topics,
                getKafkaParams(keyDeserializer, valueDeserializer)
        );

        final JavaInputDStream stream = KafkaUtils.createDirectStream(
                streamingContext,
                LocationStrategies.PreferBrokers(),
                consumerStrategy
        );

        return new Tuple2(streamingContext, stream);
    }

    private Map<String, Object> getKafkaParams(final Class keyDeserializer, final Class valueDeserializer) {
        final String securityProtocol = env.getProperty("input.securityProtocol");

        final Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", env.getProperty("input.bootstrapServers"));
        params.put("key.deserializer", keyDeserializer);
        params.put("value.deserializer", valueDeserializer);
        params.put("group.id", env.getProperty("input.groupId"));
        params.put("auto.offset.reset", env.getProperty("input.autoOffsetReset"));
        params.put("enable.auto.commit", env.getProperty("input.enableAutoCommit"));

        if (securityProtocol != null) {
            params.put("security.protocol", securityProtocol);
            params.put("ssl.truststore.location", env.getProperty("input.sslTruststore.location"));
            params.put("ssl.truststore.password", env.getProperty("input.sslTruststore.password"));
            params.put("ssl.keystore.location", env.getProperty("input.sslKeystore.location"));
            params.put("ssl.keystore.password", env.getProperty("input.sslKeystore.password"));
            params.put("ssl.key.password", env.getProperty("input.sslKey.password"));
        }

        return params;
    }

    private Boolean sendRddToKafka(JavaRDD rdd, String topic, final Class keySerializer, final Class valueSerializer) {
        try {
            rdd.foreach(r -> {
                KafkaProducer producer = new KafkaProducer(getKafkaProducerParams(keySerializer, valueSerializer));
                producer.send(new ProducerRecord(topic, r));
            });
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private Map<String, Object> getKafkaProducerParams(final Class keySerializer, final Class valueSerializer) {
        final String securityProtocol = env.getProperty("output.securityProtocol");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", env.getProperty("output.bootstrapServers"));
        kafkaParams.put("acks", env.getProperty("output.acks"));
        kafkaParams.put("retries", env.getProperty("output.retries"));
        kafkaParams.put("batch.size", env.getProperty("output.batchSize"));
        kafkaParams.put("linger.ms", env.getProperty("output.lingerMs"));
        kafkaParams.put("buffer.memory", env.getProperty("output.bufferMemory"));
        kafkaParams.put("key.serializer", keySerializer);
        kafkaParams.put("value.serializer", valueSerializer);

        if (securityProtocol != null) {
            kafkaParams.put("security.protocol", securityProtocol);
            kafkaParams.put("ssl.truststore.location", env.getProperty("output.sslTruststore.location"));
            kafkaParams.put("ssl.truststore.password", env.getProperty("output.sslTruststore.password"));
            kafkaParams.put("ssl.keystore.location", env.getProperty("output.sslKeystore.location"));
            kafkaParams.put("ssl.keystore.password", env.getProperty("output.sslKeystore.password"));
            kafkaParams.put("ssl.key.password", env.getProperty("output.sslKey.password"));
        }

        return kafkaParams;
    }
}
