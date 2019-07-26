package no.sysco.middleware.kafka.monitoring;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.streams.KeyValue;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractKafkaMetrics implements MetricsReporter {

    public abstract String getMetricPrefix();

    @Override
    public void init(List<KafkaMetric> metrics) {
        System.out.println("INITED");
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        System.out.println("REMOVED");
    }

    @Override
    public void close() {
        System.out.println("CLOSED");
    }

    @Override
    public void configure(Map<String, ?> configs) {
        System.out.println("CONFIGURED");
    }

    @Override
    public void metricChange(KafkaMetric metric) {
        MetricName metricNameRef = metric.metricName();
        String groupName = metricNameRef.group();
        String metricName = String.format("%s%s.%s", getMetricPrefix(), groupName, metricNameRef.name());
        Map<String, String> metricTags = metricNameRef.tags();
        if (!(metric.metricValue() instanceof Double)) {
            System.out.printf("Skipping non-double metric: [%s] -> [%s]\n", metricName, metric.metricValue().getClass());
            return;
        }
        Collection<KeyValue> tags = metricTags.entrySet().stream().map(e -> KeyValue.pair(e.getKey(), e.getValue()))
                .collect(Collectors.toSet());

        System.out.printf("Reporting metric %s with value %s and tags %s\n", metricName, metric.metricValue(), tags);
    }
}
