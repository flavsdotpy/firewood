package com.ap3x.firewood.services.streamsource;

import com.ap3x.firewood.common.FirewoodSpark;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Seconds;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.dstream.ReceiverInputDStream;
import org.apache.spark.streaming.kinesis.KinesisUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.IndexedSeq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

@Service(value = "kinesisService")
public class KinesisStreamService implements StreamingSourceService {

    private static final Log LOGGER = LogFactory.getLog(KinesisStreamService.class);

    @Autowired
    private Environment env;

    @Autowired
    private FirewoodSpark firewoodSpark;

    @Override
    public Tuple2<StreamingContext, IndexedSeq<ReceiverInputDStream<byte[]>>> read() {
        final SparkContext sparkContext = firewoodSpark.getSparkSession().sparkContext();

        final String appName = env.getProperty("spark.application");
        final String streamName = env.getProperty("input.stream");
        final String region = env.getProperty("input.region");
        final String endpointUrl = env.getProperty("input.endpoint");

        final AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder
                .standard().withRegion(region)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        final Integer numStreams = amazonKinesis.describeStream(streamName).getStreamDescription().getShards().size();
        final Duration kinesisCheckpointInterval = Seconds.apply(2);
        final String regionName = RegionUtils.getRegion(region).getName();

        final StreamingContext streamingContext = new StreamingContext(sparkContext, kinesisCheckpointInterval);

        final List<ReceiverInputDStream<byte[]>> kinesisStreams = new ArrayList<>();
        for (int i=0; i<numStreams; i++){
            kinesisStreams.add(KinesisUtils.createStream(streamingContext, appName, streamName, endpointUrl, regionName,
                    InitialPositionInStream.LATEST, kinesisCheckpointInterval, StorageLevel.MEMORY_AND_DISK_2()));

        }

        return new Tuple2<>(streamingContext, JavaConverters.asScalaIteratorConverter(kinesisStreams.iterator()).asScala().toIndexedSeq());
    }

    @Override
    public Boolean write(final JavaRDD rdd, final String streamName){
        final Regions region = Regions.valueOf(env.getProperty("output.region"));
        final String partitionKey = env.getProperty("output.partitionKey");

        final AmazonKinesis amazonKinesis = AmazonKinesisClientBuilder
                .standard().withRegion(region)
                .withCredentials(new DefaultAWSCredentialsProviderChain())
                .build();

        rdd.foreach(r ->  getRecordRequest(streamName, partitionKey, amazonKinesis, r));
        return true;
    }

    private PutRecordResult getRecordRequest(String streamName, String partitionKey, AmazonKinesis amazonKinesis, Object r) {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setPartitionKey(partitionKey);
        putRecordRequest.withData(ByteBuffer.wrap(r.toString().getBytes()));
        return amazonKinesis.putRecord(putRecordRequest);
    }

}
