package no.ssb.rawdata.converter.metrics;

import io.micrometer.core.instrument.Tags;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;


class MetricTest {

    @Test
    void parseMetric_withTags() {
        Metric m = new Metric("some_metric_name{tag1=val1, tag2=val2}");
        assertThat(m.getName()).isEqualTo("some_metric_name");
        assertThat(m.getTags()).isEqualTo(Tags.of("tag1", "val1", "tag2", "val2"));
        assertThat(m.getTags()).isEqualTo(Tags.of("tag2", "val2", "tag1", "val1"));
        assertThat(m.toString()).isEqualTo("some_metric_name{tag1=val1,tag2=val2}");
    }

    @Test
    void parseMetric_withoutTags() {
        Metric m = new Metric("some_metric_name");
        assertThat(m.getName()).isEqualTo("some_metric_name");
        assertThat(m.getTags()).isNull();
        assertThat(m.toString()).isEqualTo("some_metric_name");

        m = new Metric("some_metric_name{}");
        assertThat(m.getName()).isEqualTo("some_metric_name");
        assertThat(m.getTags()).isNull();
        assertThat(m.toString()).isEqualTo("some_metric_name");
    }

    @Test
    void parseMetric_invalidInput_shouldThrowExceptions() {
        assertThatExceptionOfType(NullPointerException.class)
          .isThrownBy(() -> new Metric(null));

        assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new Metric("    "));

        assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new Metric("    ", Tags.of("foo", "bar")));

        assertThatExceptionOfType(IllegalArgumentException.class)
          .isThrownBy(() -> new Metric("someMetricName"));
    }

    @Test
    void metricsWithSameTags_tagsWithDifferentOrdering_shouldBeEqual() {
        Metric m1 = new Metric("some_metric_name{tag1=val1, tag2=val2}");
        Metric m2 = new Metric("some_metric_name{tag2=val2,tag1=val1}");
        assertThat(m1.equals(m2));
    }

    @Test
    void equals() {
        Metric m1 = new Metric("some_metric_name{}");
        Metric m2 = new Metric("    some_metric_name   ");
        assertThat(m1.equals(m2));
    }

}