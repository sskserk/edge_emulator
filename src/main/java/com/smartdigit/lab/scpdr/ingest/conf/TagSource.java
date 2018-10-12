package com.smartdigit.lab.scpdr.ingest.conf;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

@JsonDeserialize(using = TagSource.TagSourceDeserializer.class)
public class TagSource implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(TagSource.class);

    @JsonProperty
    private String tagName;

    @JsonProperty
    private String calculatedTagName;

    @JsonProperty
    private String filePath;

    public TagSource(String tagName, String calculatedTagName, String filePath) {
        this.tagName = tagName;
        this.calculatedTagName = calculatedTagName;
        this.filePath = filePath;
    }

    public TagSource() {
    }

    public String getTagName() {
        return tagName;
    }

    public void setTagName(String tagName) {
        this.tagName = tagName;
    }

    public String getCalculatedTagName() {
        return calculatedTagName;
    }

    public void setCalculatedTagName(String calculatedTagName) {
        this.calculatedTagName = calculatedTagName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public String toString() {
        return "TagSource{" +
                "tagName='" + tagName + '\'' +
                ", calculatedTagName='" + calculatedTagName + '\'' +
                ", filePath='" + filePath + '\'' +
                '}';
    }

    public static class TagSourceDeserializer extends StdDeserializer<TagSource> {
        private static final Logger logger = LoggerFactory.getLogger(TagSourceDeserializer.class);

        public TagSourceDeserializer() { // not used but required
            this(null);
        }

        public TagSourceDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public TagSource deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            JsonNode node = jsonParser.getCodec().readTree(jsonParser);
            logger.info("source data {}", node.toString());
            String tagName = node.get("tagName").asText();
            String calculatedTagName = node.get("calculatedTagName").asText();
            String filePath = node.get("filePath").asText();

            return new TagSource(tagName, calculatedTagName, filePath);
        }
    }

}
