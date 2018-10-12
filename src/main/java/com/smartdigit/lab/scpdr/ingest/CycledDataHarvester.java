package com.smartdigit.lab.scpdr.ingest;

import com.smartdigit.lab.scpdr.ingest.conf.EdgeEmulatorConfiguration;
import com.smartdigit.lab.scpdr.ingest.conf.TagSource;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Service
public class CycledDataHarvester {
    private static final Logger logger = LoggerFactory.getLogger(CycledDataHarvester.class);

    @Autowired
    private EdgeEmulatorConfiguration edgeEmulatorConfiguration;

    private List<CSVRecordItem> csvParserRecords = new ArrayList<>();

    private static class CSVRecordItem {
        Iterator<CSVRecord> iteratorRecord;
        TagSource tagSource;
    }

    public void reset() throws IOException {
        for (TagSource tagSource : this.edgeEmulatorConfiguration.getTagSources()) {
            String filePath = tagSource.getFilePath();

            CSVRecordItem csvRecordItem = new CSVRecordItem();
            csvRecordItem.iteratorRecord = reinitCsvRecordIterator(filePath);
            csvRecordItem.tagSource = tagSource;

            csvParserRecords.add(csvRecordItem);
        }
    }

    private Iterator<CSVRecord> reinitCsvRecordIterator(String filePath) throws IOException {
        Reader reader = new FileReader(filePath);
        CSVParser csvParser = new CSVParser(reader, CSVFormat.DEFAULT
                .withFirstRecordAsHeader()
                .withIgnoreHeaderCase()
                .withTrim());

        return csvParser.iterator();
    }

    public TsIngestionRow readNextIngestionRow() throws IOException {

        TsIngestionRow row = new TsIngestionRow();
        row.setTs(LocalDateTime.now()); // not used, will be overwritten later with LocalDateTime.now

        for (CSVRecordItem csvRecordItem : this.csvParserRecords) {

            if (!csvRecordItem.iteratorRecord.hasNext()) { // cycle
                logger.info("no more records: reinit file reader for '{}'", csvRecordItem.tagSource.getFilePath());
                csvRecordItem.iteratorRecord = reinitCsvRecordIterator(csvRecordItem.tagSource.getFilePath());
            }

            CSVRecord csvRecord = csvRecordItem.iteratorRecord.next();
            Map<String, String> values = csvRecord.toMap();

            row.putTag(csvRecordItem.tagSource.getTagName(), Double.parseDouble(values.get("Level")));//  //.fillTagsFrom(values);

        }
        return row;
    }
}
