package com.ap3x.firewood.binary.serializers;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class KafkaAvroSerializer<T extends GenericRecord> implements Serializer<T> {

    private final EncoderFactory encoderFactory = EncoderFactory.get();

    public KafkaAvroSerializer() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, GenericRecord data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = this.encoderFactory.directBinaryEncoder(out, (BinaryEncoder) null);
            Object writer;
            if (data instanceof SpecificRecord) {
                writer = new SpecificDatumWriter(data.getSchema());
            } else {
                writer = new GenericDatumWriter(data.getSchema());
            }

            ((DatumWriter) writer).write(data, encoder);
            encoder.flush();

            byte[] bytes = out.toByteArray();
            return bytes;
        } catch (RuntimeException | IOException var9) {
            throw new SerializationException("Error serializing Avro message", var9);
        }
    }

    @Override
    public void close() {
    }
}
