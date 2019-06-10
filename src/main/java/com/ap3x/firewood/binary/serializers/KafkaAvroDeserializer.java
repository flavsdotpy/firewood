package com.ap3x.firewood.binary.serializers;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

public class KafkaAvroDeserializer<T extends GenericRecord> implements Deserializer<T> {

    public static final String AVRO_CLASSREF = "deserializer.avro.classref";

    private final DecoderFactory decoderFactory = DecoderFactory.get();

    private Class<? extends GenericRecord> recordClass;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        Object classRef = configs.get(AVRO_CLASSREF);

        this.recordClass = Optional.ofNullable(classRef)
                .map(o -> (Class<? extends GenericRecord>) o)
                .orElseThrow(() -> new IllegalArgumentException("Missing Avro class type"));
    }

    @Override
    public T deserialize(String topic, byte[] data) {

        Optional.ofNullable(recordClass)
                .orElseThrow(() -> new IllegalStateException("Deserializer didn't get properly configured. Did you call 'configure' method?"));

        try {
            T result = null;

            if (data != null) {

                DatumReader<GenericRecord> datumReader =
                        new SpecificDatumReader(recordClass);
                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);

                result = (T) datumReader.read(null, decoder);
            }
            return result;
        } catch (Exception ex) {
            throw new SerializationException(
                    "Can't deserialize data '" + Arrays.toString(data) + "' from topic '" + topic + "'", ex);
        }
    }

    @Override
    public void close() {

    }
}
