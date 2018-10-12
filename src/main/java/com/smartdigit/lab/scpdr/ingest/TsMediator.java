package com.smartdigit.lab.scpdr.ingest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.Arrays;


@Component
public class TsMediator {
    private static final Logger logger = LoggerFactory.getLogger(TsMediator.class);

    @Autowired
    private TagWindowsData windowsData;

    @Autowired
    private TsIngestor tsIngestor;

    public void publishTagsRow(TsIngestionRow row) throws IOException {
        TsIngestionRow calculatedRow = windowsData.addTagsSlice(row).extractCalculatedTags();

        tsIngestor.ingestTagsSet(Arrays.asList(row, calculatedRow));
    }
}

