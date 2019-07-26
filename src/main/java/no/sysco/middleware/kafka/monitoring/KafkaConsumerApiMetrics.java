package no.sysco.middleware.kafka.monitoring;

public class KafkaConsumerApiMetrics extends AbstractKafkaMetrics{
    @Override
    public String getMetricPrefix() {
        return "kafka.consumer.";
    }
}
