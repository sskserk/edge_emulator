package com.smartdigit.lab.test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.smartdigit.lab.scpdr.ingest.conf.EdgeEmulatorConfiguration;
import com.smartdigit.lab.scpdr.ingest.conf.EventHubConnectionProps;
import com.smartdigit.lab.scpdr.ingest.conf.TagSource;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConfYamlWriterTest {
    private static final Logger logger = LoggerFactory.getLogger(ConfYamlWriterTest.class);
    private static final String TEST_DATA_FILE = "./test_conf.yaml";



    @Test
    public void testSaveYaml() throws IOException {
        EdgeEmulatorConfiguration edgeEmulatorConfiguration = new EdgeEmulatorConfiguration();

        EventHubConnectionProps props = new EventHubConnectionProps();
        props.setIngestClientId("clientId");
        props.setIngestClientSecret("secret");
        props.setTsIngestionUrl("ingestUrl");
        props.setTsTokenObtainUrl("tokenUrl");
        props.setZoneId("zoneId");
        edgeEmulatorConfiguration.setEventHubProps(props);

        TagSource tag1 = new TagSource();
        tag1.setTagName("HELLO_WORLD_TAG");
        tag1.setCalculatedTagName("CALCULATED_TAG");
        tag1.setFilePath("/Users/sergeykim/dev/labs/threadstoptester/tank27_300th_041217_11-58.csv");
        edgeEmulatorConfiguration.addTagSource(tag1);

        TagSource tag2 = new TagSource();
        tag2.setTagName("HELLO_WORLD_TAG_2");
        tag2.setCalculatedTagName("CALCULATED_TAG_2");
        tag2.setFilePath("/Users/sergeykim/dev/labs/threadstoptester/tank25_0th_270415_15-14.csv");
        edgeEmulatorConfiguration.addTagSource(tag2);

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);

        mapper.writerWithDefaultPrettyPrinter().writeValue(new File(TEST_DATA_FILE), edgeEmulatorConfiguration);
    }

    @Test
    public void testRead() throws Exception {
        InputStream testData = new FileInputStream(new File(TEST_DATA_FILE));// this.getClass().getResourceAsStream(TEST_DATA_FILE);

        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());
       // yamlObjectMapper.configure(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY, true);
     //   yamlObjectMapper.configure(SerializationConfig.Feature.UNWRAP_ROOT_VALUE, true);
       // yamlObjectMapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);

        SimpleModule module = new SimpleModule();
        //module.addDeserializer(TagSource.class, new TagSource.TagSourceDeserializer());
        //module.addDeserializer(EventHubConnectionProps.class, new EventHubConnectionProps.EventHubConnectionPropsDeserializer());
       // module.addDeserializer(EdgeEmulatorConfiguration.class, new EdgeEmulatorConfiguration.EdgeEmulatorConfigurationDeserializer());

     //   yamlObjectMapper.registerModule(module);

      //  yamlObjectMapper.readValue(testData, EdgeEmulatorConfiguration.class);


        EdgeEmulatorConfiguration tagsMap = yamlObjectMapper.readValue(testData, EdgeEmulatorConfiguration.class);

/*
        Map<String, Object> tagsMap = yamlObjectMapper.readValue(testData, new TypeReference<HashMap>() {
        });

        for (Map.Entry<String, Object> entry : tagsMap.entrySet()) {
            switch (entry.getKey()) {
                case "eventHubConnectionProps":
                case "tagSources":
            }
            logger.info(entry.getKey().toString() +"; " + entry.getKey().getClass().getName() + ":" + entry.getValue().getClass().getName());
        }*/
//         Map tagsMap = yamlObjectMapper.readValue(testData, new TypeReference<HashMap<String, List<TagSource>>>() {
   //     });
   //     logger.info("test data {}", tagsMap);
    }
}
