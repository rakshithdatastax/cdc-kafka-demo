package com.example.cdcdemo.service;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;

final class AvroKeyDecoder {

    private AvroKeyDecoder() {
    }

    static String decodeId(byte[] bytes, KafkaAvroDeserializer keyDeserializer, String topic) throws Exception {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        if (bytes[0] == 0x0) {
            Object decoded = keyDeserializer.deserialize(topic, bytes);
            if (decoded instanceof GenericRecord record) {
                Object idField = record.get("id");
                return idField == null ? null : idField.toString();
            }
            return decoded == null ? null : decoded.toString();
        }
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericDatumReader<Object> reader = new GenericDatumReader<>(stringSchema);
        return reader.read(null, decoder).toString();
    }
}
