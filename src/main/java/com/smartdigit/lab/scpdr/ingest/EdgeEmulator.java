package com.smartdigit.lab.scpdr.ingest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.smartdigit.lab.scpdr.ingest.conf.EdgeEmulatorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;
import org.springframework.context.annotation.Bean;

import java.io.File;
import java.io.IOException;


@SpringBootApplication
public class EdgeEmulator extends SpringBootServletInitializer implements ApplicationRunner {
    private static final Logger logger = LoggerFactory.getLogger(EdgeEmulator.class);

    @Autowired
    private TsMediator tsMediator;

    @Autowired
    private CycledDataHarvester cycledDataHarvester;

    @Bean(name = "edgeEmulatorConfiguration")
    public EdgeEmulatorConfiguration buildEdgeEmulatorConfiguration(@Value("${conf}") String configFilePath) throws IOException {
        ObjectMapper yamlObjectMapper = new ObjectMapper(new YAMLFactory());

        EdgeEmulatorConfiguration edgeEmulatorConfiguration = yamlObjectMapper.readValue(new File(configFilePath), EdgeEmulatorConfiguration.class);
        logger.info("Used configuration {}", edgeEmulatorConfiguration);

        return edgeEmulatorConfiguration;
    }

    public static void main(String[] args) {
        SpringApplication.run(EdgeEmulator.class, args);
    }

    @Override
    public void run(ApplicationArguments applicationArguments) throws IOException {
        logger.info("The application is about to start");

        this.doIngestionEmulation();
    }

    private void doIngestionEmulation() throws IOException {
        cycledDataHarvester.reset();
        while (true) {
            try {
                final TsIngestionRow row = cycledDataHarvester.readNextIngestionRow();
                this.tsMediator.publishTagsRow(row);

                Thread.sleep(1000); // simply wait 59 seconds
                // logger.error("Error on ingestion iteration", ex);
            } catch (InterruptedException ex) {
                logger.error("Ingestion error", ex);
            }
        }
    }
}
