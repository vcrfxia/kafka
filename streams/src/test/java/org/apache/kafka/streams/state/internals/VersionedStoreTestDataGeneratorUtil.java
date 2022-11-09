package org.apache.kafka.streams.state.internals;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class VersionedStoreTestDataGeneratorUtil {
    // timestamps are random with uniform distribution from 0 to maxTimestamp, for a single key
    static List<DataRecord> generateTestRecords(
        final long maxTimestamp, final int numRecords, final String key) {
        final List<DataRecord> records = new ArrayList<>();

        final long tsScalar = Math.max(maxTimestamp / numRecords, 1);
        for (int i = 0; i < numRecords; i++) {
            // random timestamp, with scalar to help increase chance of collisions
            final long timestamp = (int)(Math.random() * Math.min(numRecords, maxTimestamp)) * tsScalar;

            // random value, with some probability of null
            final String value = Math.random() < 0.2 ? null : "v" + i;

            records.add(new DataRecord(key, value, timestamp));
        }
        return records;
    }

    // assumes all records are for the same key.
    // does not take history retention into account; caller should recognize that results which
    // have fallen out of history retention are not guaranteed
    static Map<Long, DataRecord> computeTestCases(
        final List<DataRecord> records) {
        final Map<Long, DataRecord> testCases = new HashMap<>();

        // add timestamps corresponding to the data points themselves
        for (DataRecord record : records) {
            testCases.put(record.timestamp, record);
        }

        // collect and sort (unique) timestamps
        final List<Long> timestamps = new ArrayList<>(testCases.keySet());
        Collections.sort(timestamps);

        // add timestamps for one greater than data points
        for (Long timestamp : timestamps) {
            testCases.putIfAbsent(timestamp + 1, testCases.get(timestamp));
        }

        // add timestamps for one less than data points
        if (timestamps.get(0) > 0) {
            testCases.put(timestamps.get(0) - 1, new DataRecord(testCases.get(timestamps.get(0)).key, null));
        }
        for (int i = 1; i < timestamps.size(); i++) {
            testCases.putIfAbsent(timestamps.get(i) - 1, testCases.get(timestamps.get(i-1)));
        }

        // TODO: also return expected value for query without time filter
        return testCases;
    }

    static List<DataRecord> getRecordsFromFile(final Class clazz, final String filename) {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(
            clazz.getClassLoader().getResourceAsStream(filename)
        ))) {
            return br.lines().map(l -> {
                String[] parts = l.split(",");
                String valueStr = parts[2].substring(parts[2].indexOf("value = ") + "value = ".length());
                return new DataRecord(
                    parts[1].substring(parts[1].indexOf("key = ") + "key = ".length()),
                    valueStr.equals("null") ? null : valueStr,
                    Long.parseLong(parts[0].substring(parts[0].indexOf("ts = ") + "ts = ".length()))
                );
            }).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("failed to load test records");
        }
    }

    static String getGeneratedTestCaseFailureMessage(
        final List<DataRecord> records, final Map.Entry<Long, DataRecord> testCase, final String failedProperty) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Failed assertion for generated records:\n");
        for (DataRecord record : records) {
            sb.append(String.format("\tts = %d, key = %s, value = %s\n", record.timestamp, record.key, record.value));
        }
        sb.append(String.format("Checking for ts = %d, value = %s\n", testCase.getKey(), testCase.getValue() != null ? testCase.getValue() : "null"));
        sb.append(String.format("%s is incorrect.", failedProperty));
        return sb.toString();
    }

    static List<String> getSavedDataFilenames() {
        final List<String> files = new ArrayList<>();
        files.add("versioned_store_test/test_records.txt");
        files.add("versioned_store_test/test_records_2.txt");
        files.add("versioned_store_test/test_records_3.txt"); // hr, si, mgts: 250, 100, 300
        return files;
    }

    static class DataRecord {
        final String key;
        final String value;
        final long timestamp;

        DataRecord(String key, String value) {
            this(key, value, -1L);
        }

        DataRecord(String key, String value, long timestamp) {
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }
    }
}
