package com.ap3x.firewood.binary.serializers;

import com.ap3x.firewood.binary.schemas.KafkaFlowRecord;
import com.ap3x.firewood.binary.schemas.KafkaMetadataRecord;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;


public class KafkaAvroSerializerTest {

    private KafkaFlowRecord kafkaFlowRecord;
    private KafkaAvroSerializer<KafkaFlowRecord> subject;

    @Before
    public void setUp() {
        kafkaFlowRecord = KafkaFlowRecord.newBuilder()
                .setClazz("SKU")
                .setType("1301")
                .setMetadata(KafkaMetadataRecord.newBuilder()
                        .setObjectURI("gs://bucket/directory/file.txt")
                        .setFilename("file")
                        .setVersion(2L)
                        .setRecordId("1234241").build()
                )
                .setValues(new HashMap<>()).build();

        subject = new KafkaAvroSerializer<KafkaFlowRecord>();
    }

    @Test
    public void testSerialization() throws Exception {

        byte[] data = subject.serialize(null, kafkaFlowRecord);

        assertEquals(deserialize(data), kafkaFlowRecord);
    }

    private KafkaFlowRecord deserialize(byte[] data) throws IOException {
        DatumReader<GenericRecord> datumReader =
                new SpecificDatumReader<>(KafkaFlowRecord.getClassSchema());
        Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

        return (KafkaFlowRecord) datumReader.read(null, decoder);
    }
}
