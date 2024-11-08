/*
 * Copyright 2024, AutoMQ HK Limited.
 *
 * The use of this file is governed by the Business Source License,
 * as detailed in the file "/LICENSE.S3Stream" included in this repository.
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

package kafka.automq.table;

import kafka.automq.table.events.AvroCodec;
import kafka.automq.table.events.Envelope;
import kafka.automq.table.events.Event;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

public class Channel {
    private static final Logger LOGGER = LoggerFactory.getLogger(Channel.class);
    private static final String CONTROL_TOPIC = "__automq_table_control";
    private static final String DATA_TOPIC = "__automq_table_data";
    private final KafkaProducer<byte[], byte[]> producer;

    public Channel() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(props);
    }

    public void send(Event event) throws Exception {
        // TODO: send to partition which hash(topic) % partition num
        switch (event.type()) {
            case COMMIT_REQUEST:
                producer.send(new ProducerRecord<>(CONTROL_TOPIC, 0, null, AvroCodec.encode(event))).get();
                break;
            case COMMIT_RESPONSE:
                producer.send(new ProducerRecord<>(DATA_TOPIC, 0, null, AvroCodec.encode(event))).get();
                break;
            default:
                throw new IllegalArgumentException("Unknown event type: " + event.type());
        }
    }

    public SubChannel subscribeControl() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "automq-table-control-consumer");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.assign(List.of(new TopicPartition(CONTROL_TOPIC, 0)));
        Queue<Envelope> left = new LinkedBlockingQueue<>();
        return () -> {
            if (!left.isEmpty()) {
                return left.poll();
            }
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(record -> {
                try {
                    Event event = AvroCodec.decode(record.value());
                    left.add(new Envelope(record.partition(), record.offset(), event));
                } catch (IOException e) {
                    LOGGER.error("decode fail");
                }
            });
            return left.poll();
        };
    }

    public SubChannel subscribeData(String topic, long offset) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "automq-table-control-consumer");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
        consumer.assign(List.of(new TopicPartition(DATA_TOPIC, 0)));
        consumer.seek(new TopicPartition(DATA_TOPIC, 0), offset);
        Queue<Envelope> left = new LinkedBlockingQueue<>();
        return () -> {
            if (!left.isEmpty()) {
                return left.poll();
            }
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofSeconds(1));
            records.forEach(record -> {
                try {
                    Event event = AvroCodec.decode(record.value());
                    left.add(new Envelope(record.partition(), record.offset(), event));
                } catch (IOException e) {
                    LOGGER.error("decode fail");
                }
            });
            return left.poll();
        };
    }

    public interface SubChannel {
        Envelope poll();
    }

}
