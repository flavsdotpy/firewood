package com.ap3x.firewood.binary.serializers;

import com.ap3x.firewood.binary.schemas.KafkaFlowRecord;
import com.ap3x.firewood.binary.schemas.KafkaMetadataRecord;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;

public class KafkaAvroDeserializerTest {

    @Rule
    public ExpectedException exceptions = ExpectedException.none();

    private KafkaAvroDeserializer<KafkaFlowRecord> subject;
    private byte[] byteArray;
    private KafkaFlowRecord kafkaFlowRecord;

    private Map<String, Class<? extends GenericRecord>> configs = new HashMap<>();
    private Consumer<Class<? extends GenericRecord>> configFn = clazz -> {
        configs.put(KafkaAvroDeserializer.AVRO_CLASSREF, clazz);
        subject.configure(configs, false);
    };

    @Before
    public void setUp() throws IOException {
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

        byteArray = serializer(kafkaFlowRecord);

        subject = new KafkaAvroDeserializer<KafkaFlowRecord>();
    }

    @Test
    public void testMissingAvroClassOnConfiguration() throws Exception {
        exceptions.expectMessage("Missing Avro class type");
        configFn.accept(null);
    }

    @Test
    public void testDeserializationWithoutPreviousCallOnConfigureMethod() throws Exception {
        exceptions.expectMessage("Deserializer didn't get properly configured. Did you call 'configure' method?");
        subject.deserialize(null, byteArray);
    }

    @Test
    public void testDeserialization() throws Exception {
        configFn.accept(KafkaFlowRecord.class);

        KafkaFlowRecord avro = subject.deserialize(null, byteArray);

        assertEquals(avro, kafkaFlowRecord);
    }

    private byte[] serializer(KafkaFlowRecord data) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, (BinaryEncoder) null);
        GenericDatumWriter writer = new GenericDatumWriter(data.getSchema());

        ((DatumWriter) writer).write(data, encoder);
        encoder.flush();

        byte[] bytes = out.toByteArray();
        out.close();
        return bytes;
    }
}
