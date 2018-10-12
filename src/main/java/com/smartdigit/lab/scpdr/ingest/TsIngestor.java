package com.smartdigit.lab.scpdr.ingest;

import com.ge.predix.timeseries.client.ClientFactory;
import com.ge.predix.timeseries.client.TenantContext;
import com.ge.predix.timeseries.client.TenantContextFactory;
import com.ge.predix.timeseries.exceptions.PredixTimeSeriesException;
import com.ge.predix.timeseries.model.response.IngestionResponse;
import com.smartdigit.lab.scpdr.ingest.conf.EdgeEmulatorConfiguration;
import com.smartdigit.lab.scpdr.ingest.conf.EventHubConnectionProps;
import com.smartdigit.lab.uaa.UaaTokenManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;

@Component
public class TsIngestor {
    private static final Logger logger = LoggerFactory.getLogger(TsIngestor.class);
    private static final String ZONE_HEADER_NAME = "Predix-Zone-Id";

    @Autowired
    private EdgeEmulatorConfiguration edgeEmulatorConfiguration;


    private UaaTokenManager uaaTokenManager;

    public TsIngestor() {
    }

    public void ingestTagsSet(List<TsIngestionRow> row) throws IOException {
        final EventHubConnectionProps eventHubProps = this.edgeEmulatorConfiguration.getEventHubProps();

        logger.info("token obtain url {}", eventHubProps.getTsTokenObtainUrl());
        uaaTokenManager = new UaaTokenManager(eventHubProps.getTsTokenObtainUrl());

        final String authIngestToken = uaaTokenManager.obtainToken(eventHubProps.getIngestClientId(), eventHubProps.getIngestClientSecret()); // TODO: implement token caching

        long insertTs = LocalDateTime.now().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli();

        String ingestionJson = TsUtil.createIngestionJsonForTs(System.currentTimeMillis(), insertTs, row);
        try {
            final TenantContext ingestTenant =
                    TenantContextFactory
                            .createIngestionTenantContextFromProvidedProperties(eventHubProps.getTsIngestionUrl(),
                                    authIngestToken,
                                    ZONE_HEADER_NAME,
                                    eventHubProps.getZoneId());

            IngestionResponse response = ClientFactory.ingestionClientForTenant(ingestTenant).ingest(ingestionJson);

            logger.info("message id {} / {} / response:{}", System.currentTimeMillis(), ingestionJson, response.getStatusCode());
        } catch (PredixTimeSeriesException | URISyntaxException e) {
            logger.error("Error on ingestion", e);
            throw new IOException(e);
        }
    }
}
