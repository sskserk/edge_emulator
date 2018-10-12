package com.smartdigit.lab.test;

import com.smartdigit.lab.scpdr.ingest.EdgeEmulator;
import com.smartdigit.lab.scpdr.ingest.TagWindowsData;
import com.smartdigit.lab.scpdr.ingest.TsIngestionRow;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertTrue;

public class TagWindowDataTest {
    private static final Logger logger = LoggerFactory.getLogger(TagWindowDataTest.class);

//    @Test
    public void testTagWindow() {


        TagWindowsData tagWindowsData = new TagWindowsData();

        TsIngestionRow row = new TsIngestionRow();
        row.putTag("one",1D);
        TsIngestionRow ingestionRow = tagWindowsData.addTagsSlice(row).extractCalculatedTags();
        logger.info("tags {}", ingestionRow.getTags());
        assertTrue(ingestionRow.getTags().get("oneC") == 0D);

        row.putTag("one",2D);
        logger.info("{}", tagWindowsData.addTagsSlice(row).extractCalculatedTags());

        row.putTag("one",3D);
        logger.info("{}", tagWindowsData.addTagsSlice(row).extractCalculatedTags());

    }



}
