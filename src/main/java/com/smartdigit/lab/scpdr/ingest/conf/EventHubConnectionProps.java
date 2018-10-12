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

@JsonDeserialize(using = EventHubConnectionProps.EventHubConnectionPropsDeserializer.class)
public class EventHubConnectionProps implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(EventHubConnectionProps.class);

    @JsonProperty("tsTokenObtainUrl")
    private String tsTokenObtainUrl;

    @JsonProperty("tsIngestionUrl")
    private String tsIngestionUrl;

    @JsonProperty("zoneId")
    private String zoneId;

    @JsonProperty("ingestClientId")
    private String ingestClientId;

    @JsonProperty("ingestClientSecret")
    private String ingestClientSecret;

    public String getTsTokenObtainUrl() {
        return tsTokenObtainUrl;
    }

    public void setTsTokenObtainUrl(String tsTokenObtainUrl) {
        this.tsTokenObtainUrl = tsTokenObtainUrl;
    }

    public String getTsIngestionUrl() {
        return tsIngestionUrl;
    }

    public void setTsIngestionUrl(String tsIngestionUrl) {
        this.tsIngestionUrl = tsIngestionUrl;
    }

    public String getZoneId() {
        return zoneId;
    }

    public void setZoneId(String zoneId) {
        this.zoneId = zoneId;
    }

    public String getIngestClientId() {
        return ingestClientId;
    }

    public void setIngestClientId(String ingestClientId) {
        this.ingestClientId = ingestClientId;
    }

    public String getIngestClientSecret() {
        return ingestClientSecret;
    }

    public void setIngestClientSecret(String ingestClientSecret) {
        this.ingestClientSecret = ingestClientSecret;
    }

    @Override
    public String toString() {
        return "EventHubConnectionProps{" +
                "tsTokenObtainUrl='" + tsTokenObtainUrl + '\'' +
                ", tsIngestionUrl='" + tsIngestionUrl + '\'' +
                ", zoneId='" + zoneId + '\'' +
                ", ingestClientId='" + ingestClientId + '\'' +
                ", ingestClientSecret='" + ingestClientSecret + '\'' +
                '}';
    }

    public static class EventHubConnectionPropsDeserializer extends StdDeserializer<EventHubConnectionProps> {

        public EventHubConnectionPropsDeserializer() { // not used but required
            this(null);
        }

        public EventHubConnectionPropsDeserializer(Class<?> vc) {
            super(vc);
        }

        @Override
        public EventHubConnectionProps deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException, JsonProcessingException {
            JsonNode node = jsonParser.getCodec().readTree(jsonParser);
            logger.info("source data  {}", node.toString());
            //JsonNode propsNode = node.get("eventHubConnectionProps");

            String tsTokenObtainUrl = node.get("tsTokenObtainUrl").asText();
            String tsIngestionUrl = node.get("tsIngestionUrl").asText();
            String zoneId = node.get("zoneId").asText();
            String ingestClientId = node.get("ingestClientId").asText();
            String ingestClientSecret = node.get("ingestClientSecret").asText();

            EventHubConnectionProps props = new EventHubConnectionProps();
            props.setTsTokenObtainUrl(tsTokenObtainUrl);
            props.setTsIngestionUrl(tsIngestionUrl);
            props.setZoneId(zoneId);
            props.setIngestClientId(ingestClientId);
            props.setIngestClientSecret(ingestClientSecret);

            return props;
        }
    }
}
