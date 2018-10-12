package com.smartdigit.lab.test;

import com.smartdigit.lab.scpdr.ingest.EdgeEmulator;
import com.smartdigit.lab.scpdr.ingest.TsIngestionRow;
import com.smartdigit.lab.scpdr.ingest.TsUtil;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.LocalDateTime;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TsUtilTest {

    private static final Logger logger = LoggerFactory.getLogger(EdgeEmulator.class);

    @Test
    public void fff() {
        logger.info("hello {} world {}");
    }
}
