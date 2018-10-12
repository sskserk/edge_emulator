package com.smartdigit.lab.scpdr.ingest;

import com.smartdigit.lab.scpdr.ingest.conf.EdgeEmulatorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;


@Component
public class TagWindowsData {
    private static final Logger logger = LoggerFactory.getLogger(EdgeEmulator.class);
    private static final int WINDOW_SIZE = 10;

    @Autowired
    private EdgeEmulatorConfiguration edgeEmulatorConfiguration;

    Map<String, LinkedList<Double>> tagWindowsData = new HashMap<>();

    public TagWindowsData addTagsSlice(TsIngestionRow row) {

        for (Map.Entry<String, Double> tagEntry : row.getTags().entrySet()) {
            String tagName = tagEntry.getKey();
            Double value = tagEntry.getValue();

            LinkedList<Double> windowValues = tagWindowsData.get(tagName);
            if (windowValues == null) {
                windowValues = new LinkedList<>();
                tagWindowsData.put(tagName, windowValues);
            }
            windowValues.addLast(value);
            if (windowValues.size() > WINDOW_SIZE) {
                windowValues.removeFirst();
            }
        }

        return this;
    }

    /**
     * Y = среднее ( последние 10 значений по тегу X) - плавающее окно
     * Z  = abs (X – Y)) ; Значение результирующего тэга
     * <p>
     * <p>
     * <p>
     * исходный тег           расчетный тег
     * UNH.UTP.Rez.1012.L     UNH.UTP.Rez.1012.C
     */
    public TsIngestionRow extractCalculatedTags() {
        TsIngestionRow calculatedTagsRow = new TsIngestionRow();
        calculatedTagsRow.setTs(LocalDateTime.now());

        for (Map.Entry<String, LinkedList<Double>> tagWindow : tagWindowsData.entrySet()) {
            String originalTagName = tagWindow.getKey();
            LinkedList<Double> valuesWindow = tagWindow.getValue();
            final Double latestValue = valuesWindow.getLast();

            Optional<Double> calculatedSumTagValueOpt = valuesWindow.parallelStream().reduce(Double::sum).map(Math::abs);

            logger.debug("sum {}, last {}", calculatedSumTagValueOpt.get(), latestValue);
            double calculatedTagValue = 0D;
            if (calculatedSumTagValueOpt.isPresent())
                calculatedTagValue = Math.abs(latestValue - calculatedSumTagValueOpt.get() / valuesWindow.size());

            String calculatedTagName = this.edgeEmulatorConfiguration.getCalculatedTagNameFor(originalTagName);
            calculatedTagsRow.putTag(calculatedTagName, calculatedTagValue);
        }

        return calculatedTagsRow;
    }
}
