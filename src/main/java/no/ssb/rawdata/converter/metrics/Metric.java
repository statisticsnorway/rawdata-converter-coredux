package no.ssb.rawdata.converter.metrics;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import lombok.NonNull;
import lombok.Value;

import java.util.Map;
import java.util.stream.Collectors;

/**
 * Representation of a metric and additional dimensions (tags). Provides methods
 * for converting between string representations and micrometer instruments.
 */
@Value
public class Metric {

    private final String name;
    private final Tags tags;

    public Metric(@NonNull String s) {
        int tagStart = s.indexOf('{');

        // no tags
        if (tagStart == -1) {
            this.name = checkMetricName(s);
            this.tags = null;
        }

        // with tags
        else {
            this.name = checkMetricName(s.substring(0, tagStart));
            String tagsFragment = s.substring(tagStart);
            tags = tagsOf(tagsFragment);
        }
    }

    public Metric(@NonNull String name, String... tags) {
        this(name, Tags.of(tags));
    }

    public Metric(@NonNull String name, Map<String,String> tagMap) {
        this(name, tagsOf(tagMap));
    }

    public Metric(@NonNull String name, Tags tags) {
        this.name = checkMetricName(name);
        this.tags = tags;
    }

    public String getFullName() {
        return toString();
    }

    @Override
    public String toString() {
        return (tags == null)
          ? name
          : String.format("%s{%s}", name, commaSeparated(tags));
    }

    private static String checkMetricName(@NonNull String name) {
        if (name.isBlank()) {
            throw new IllegalArgumentException("Metric name cannot be blank");
        }
        if (! name.toLowerCase().equals(name)) {
            throw new IllegalArgumentException("Metric name must be all lowercase and dot.cased");
        }

        return name;
    }

    /**
     * Transform a Tags object into a comma separated key=value string
     */
    private static String commaSeparated(Tags tags) {
        if (tags == null) {
            return "";
        }
        return tags.stream()
          .map(t -> t.getKey()+ "=" + t.getValue())
          .collect(Collectors.joining(","));
    }

    /**
     * Transform a Map to a Tags object
     */
    private static Tags tagsOf(Map<String, String> map) {
        if (map == null || map.isEmpty()) {
            return null;
        }
        return Tags.of(map.entrySet().stream()
          .map(e -> Tag.of(e.getKey(), e.getValue()))
          .collect(Collectors.toSet())
        );
    }

    /**
     * <p>Transform a comma separated key-value string to a Tags object.</p>
     *
     * <p>This is the inverse operation of {@link #commaSeparated(Tags)}.</p>
     *
     * @param keyValueString e.g. "key1=val1, key2=val2"
     * @return micrometer Tags or null if no tags found (e.g. if string is null or empty)
     */
    private static Tags tagsOf(String keyValueString) {
        try {
            keyValueString = CharMatcher.anyOf("{} ").removeFrom(keyValueString).trim();
            Map<String, String> tagsMap = Splitter.on(",").withKeyValueSeparator("=").split(keyValueString);
            return tagsOf(tagsMap);
        } catch (Exception e) {
            return null;
        }
    }

}
