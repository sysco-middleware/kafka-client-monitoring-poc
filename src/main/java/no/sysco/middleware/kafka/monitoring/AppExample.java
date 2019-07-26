package no.sysco.middleware.kafka.monitoring;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class AppExample {
    public static void main(String[] args) {
        String id1 = UUID.randomUUID().toString();
        String group1 = UUID.randomUUID().toString();

        final String topic = "topic-1";

        final KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(getConsumerProps(id1, group1));

        kafkaConsumer.subscribe(Collections.singleton(topic));
        while (true) {
            ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : poll) {
                System.out.println(record.key());
            }
            kafkaConsumer.commitAsync();
        }
    }

    static Properties getConsumerProps(String id, String groupId) {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, id);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, "no.sysco.middleware.kafka.monitoring.KafkaConsumerApiMetrics");
        return properties;
    }

    static void printMetrics(Map<MetricName, ? extends Metric> metrics) {
        for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
            System.out.println(entry.getKey());
            KafkaMetric value = (KafkaMetric) entry.getValue();
            System.out.println(value.metricValue());
            System.out.println();
        }
    }
}
