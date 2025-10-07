/**
 * @license
 * Copyright CERN and copyright holders of ALICE O2. This software is
 * distributed under the terms of the GNU General Public License v3 (GPL
 * Version 3), copied verbatim in the file "COPYING".
 *
 * See http://alice-o2.web.cern.ch/license for full licensing information.
 *
 * In applying this license CERN does not waive the privileges and immunities
 * granted to it by virtue of its status as an Intergovernmental Organization
 * or submit itself to any jurisdiction.
 */

package alice.dip.kafka;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Properties;

/**
 * Generic Kafka Producer interface to send messages to a specified topic.
 * @param <K> - Type of the message key (to be used for partitioning)
 * @param <V> - Type of the message value (payload)
 */
public class KafkaProducerInterface<K, V> implements AutoCloseable {
    private final KafkaProducer<K, V> producer;
    private final String topic;

    /**
     * Constructor to create a KafkaProducerInterface
     * @param bootstrapServers - Kafka bootstrap servers connection string in format of host:port
     * @param topic - Kafka topic to which messages will be sent
     * @param keySerializer - Kafka supported serializer for the message key
     * @param valueSerializer - Kafka supported serializer for the message value
     */
    public KafkaProducerInterface(String bootstrapServers, String topic, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.topic = topic;
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        this.producer = new KafkaProducer<>(props, keySerializer, valueSerializer);
    }

    /**
     * Send a message to the configured Kafka topic
     * @param key - message key for partitioning
     * @param value - message value (payload)
     */
    public void send(K key, V value) {
        ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
        producer.send(record);
    }

    /**
     * Method to close the Kafka producer instance
     */
    @Override
    public void close() {
        producer.close();
    }
}
