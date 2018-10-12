package com.smartdigit.lab.scpdr.ingest.conf;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
@JsonPropertyOrder({"eventHubConnectionProps", "tagSources"})
@JsonAutoDetect
@JsonDeserialize(using = EdgeEmulatorConfiguration.EdgeEmulatorConfigurationDeserializer.class)
public class EdgeEmulatorConfiguration implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(EdgeEmulatorConfiguration.class);


    @JsonProperty("eventHubConnectionProps")
    private EventHubConnectionProps eventHubProps = new EventHubConnectionProps();

    @JsonProperty("tagSources")

    private Map<String, TagSource> tagSources = new HashMap<>();


    public EdgeEmulatorConfiguration() {
    }

    public EventHubConnectionProps getEventHubProps() {
        return eventHubProps;
    }

    public void setEventHubProps(EventHubConnectionProps eventHubProps) {
        this.eventHubProps = eventHubProps;
    }

    public void setTagSources(Map<String, TagSource> tagSources) {
        this.tagSources = tagSources;
    }

    public void addTagSource(TagSource tagSource) {
        this.tagSources.put(tagSource.getTagName(), tagSource);
    }

    public Collection<TagSource> getTagSources() {
        return this.tagSources.values();
    }

    public String getCalculatedTagNameFor(String tagName) {
        return this.tagSources.get(tagName).getCalculatedTagName();
    }

    public static class EdgeEmulatorConfigurationDeserializer extends StdDeserializer<EdgeEmulatorConfiguration> {
        private static final Logger logger = LoggerFactory.getLogger(EdgeEmulatorConfigurationDeserializer.class);

        public EdgeEmulatorConfigurationDeserializer() {
            this(null);
        }

        public EdgeEmulatorConfigurationDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public EdgeEmulatorConfiguration deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            ObjectCodec codec = jsonParser.getCodec();
            final JsonNode node = codec.readTree(jsonParser);

            EdgeEmulatorConfiguration edgeEmulatorConfiguration = new EdgeEmulatorConfiguration();

            // event hub
            JsonNode propsNode = node.get("eventHubConnectionProps");
            EventHubConnectionProps connProps = codec.treeToValue(propsNode, EventHubConnectionProps.class);
            edgeEmulatorConfiguration.setEventHubProps(connProps);
            logger.debug("parsed props: {}", connProps);

            // tag sources
            JsonNode tagSourcesData = node.get("tagSources");
            for (JsonNode tagNode : tagSourcesData) {
                TagSource tagSource = codec.treeToValue(tagNode, TagSource.class);
                logger.debug("parsed tag source {}", tagSource);
                edgeEmulatorConfiguration.addTagSource(tagSource);
            }

            return edgeEmulatorConfiguration;
        }
    }

    @Override
    public String toString() {
        return "EdgeEmulatorConfiguration{" +
                "tagSources=" + tagSources +
                '}';
    }
}
