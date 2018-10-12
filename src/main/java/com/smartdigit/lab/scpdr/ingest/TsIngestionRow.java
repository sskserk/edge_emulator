package com.smartdigit.lab.scpdr.ingest;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class TsIngestionRow {
    private LocalDateTime ts;
    private Map<String, Double> tags = new HashMap<>();

    public void setTs(LocalDateTime ts) {
        this.ts = ts;
    }

    public LocalDateTime getTs() {
        return ts;
    }

    public void putTag(String tag, Double value) {
        tags.put(tag, value);
    }

    public Map<String, Double> getTags() {
        return this.tags;
    }

    public double getTagValue(String tagName) {
        return this.tags.get(tagName);
    }

    public void fillTagsFrom(Map<String, String> values) {
        values.entrySet().stream().filter(entry -> !entry.getKey().endsWith("TIME"))
                .forEach(entry -> tags.put(entry.getKey(), Double.parseDouble(entry.getValue())));
    }

    @Override
    public String toString() {
        return "TsIngestionRow{" +
                "ts=" + ts +
                ", tags=" + tags +
                '}';
    }
}
