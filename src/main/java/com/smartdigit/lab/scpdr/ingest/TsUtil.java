package com.smartdigit.lab.scpdr.ingest;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class TsUtil {


    /*
    private static String buildIngestionJson(String tagName, long tagTimestamp, Double value) throws IOException {
        String ingestJsonTemplate = IOUtils.toString(TsUtil.class.getResourceAsStream("/single_ingestion_template.json"));
        StringBuilder requestFragment = new StringBuilder();

        requestFragment.append("[").append(tagTimestamp).append(",");
        requestFragment.append(value);
        requestFragment.append(",3").append("]");

        ingestJsonTemplate = ingestJsonTemplate.replace("%TAG_NAME%", tagName)
                .replace("%DATA_POINTS%", requestFragment.toString())
                .replace("%MESSAGE_ID%", Long.toString(System.currentTimeMillis()));

        return ingestJsonTemplate;
    }*/

    public static String createIngestionJsonForTs(long messageId, long ts, List<TsIngestionRow> rows) throws IOException {
        String ingestJsonTemplate = IOUtils.toString(TsUtil.class.getResourceAsStream("/multiple_ingestion_template.json"));

        StringBuilder ingestionContent = new StringBuilder();
        int tagNumber = 0;
        int totalTags = rows.parallelStream().map(row -> row.getTags().size()).reduce(Integer::sum).get();

        for (TsIngestionRow row : rows) {
            for (Map.Entry<String, Double> entry : row.getTags().entrySet()) {
                String tagName = entry.getKey();
                Double value = entry.getValue();

                String nameJson = String.format("\"name\": \"%s\"", tagName);
                String dataPointsJson = String.format("\"datapoints\": [[%d,%f,3]]", ts, value);

                String tagData = String.format("{%s,%s}", nameJson, dataPointsJson);

                ingestionContent.append(tagData);
                if (tagNumber++ < totalTags - 1)
                    ingestionContent.append(",");
            }
        }

        return ingestJsonTemplate.replace("%CONTENT%", ingestionContent.toString()).replace("%MESSAGE_ID%", Long.toString(messageId));
    }
/*
    public static String createIngestionJson(long messageId, TsIngestionRow row, int shiftDays) throws IOException {
        String ingestJsonTemplate = IOUtils.toString(TsUtil.class.getResourceAsStream("/multiple_ingestion_template.json"));

        LocalDateTime localDateTime = row.getTs().plusDays(shiftDays);
        long ts = localDateTime.atZone(ZoneId.of("UTC")).toInstant().toEpochMilli() + 10;

        StringBuilder ingestionContent = new StringBuilder();
        int tagNumber = 0;
        int totalTags = row.getTags().size();
        for (Map.Entry<String, Double> entry : row.getTags().entrySet()) {
            String tagName = entry.getKey();
            Double value = entry.getValue();

            String nameJson = String.format("\"name\": \"%s\"", tagName);
            String dataPointsJson = String.format("\"datapoints\": [[%d,%f,3]]", ts, value);

            String tagData = String.format("{%s,%s}", nameJson, dataPointsJson);

            ingestionContent.append(tagData);
            if (tagNumber++ < totalTags - 1)
                ingestionContent.append(",");
        }

        return ingestJsonTemplate.replace("%CONTENT%", ingestionContent.toString()).replace("%MESSAGE_ID%", Long.toString(messageId));
    }
*/
}
