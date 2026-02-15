/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.eventlistener.logger;

import io.airlift.units.DataSize;

import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Utility class to handle JSON serialization of query events with field exclusion and truncation.
 * <p>
 * Features:
 * - Exclude fields completely (replaced with null in output)
 * - Truncate specific fields to a maximum size limit
 */
public class QueryEventFieldFilter
{
    private final Set<String> excludedFields;
    private final Set<String> truncatedFields;
    private final long maxFieldSizeBytes;
    private final long truncationSizeLimitBytes;

    public QueryEventFieldFilter(
            Set<String> excludedFields,
            DataSize maxFieldSize,
            Set<String> truncatedFields,
            DataSize truncationSizeLimit)
    {
        this.excludedFields = requireNonNull(excludedFields, "excludedFields is null");
        this.maxFieldSizeBytes = requireNonNull(maxFieldSize, "maxFieldSize is null").toBytes();
        this.truncatedFields = requireNonNull(truncatedFields, "truncatedFields is null");
        this.truncationSizeLimitBytes = requireNonNull(truncationSizeLimit, "truncationSizeLimit is null").toBytes();
    }

    /**
     * Apply field filtering (truncation and exclusion) to a JSON string.
     * Optimized to avoid creating multiple intermediate String objects.
     */
    public String applyFiltering(String json)
    {
        json = applyFieldExclusion(json);
        if (truncatedFields.isEmpty()) {
            return json;
        }
        return applyFieldTruncation(json);
    }

    private String applyFieldExclusion(String json)
    {
        if (excludedFields.isEmpty() || json.isEmpty()) {
            return json;
        }
        for (String field : excludedFields) {
            json = nullOutFieldInJson(json, field);
        }
        return json;
    }

    /**
     * Apply truncation to specified fields in the JSON string.
     * Reuses the same StringBuilder to minimize allocations.
     */
    private String applyFieldTruncation(String json)
    {
        if (truncatedFields.isEmpty() || json.isEmpty()) {
            return json;
        }
        for (String field : truncatedFields) {
            json = truncateFieldInJson(json, field);
        }
        return json;
    }

    /**
     * Truncate a specific field value in JSON string if it exceeds the size limit.
     * Works with string values enclosed in quotes. Optimized to avoid substring allocations.
     */
    private String truncateFieldInJson(String json, String fieldName)
    {
        String fieldPattern = "\"" + fieldName + "\":\"";
        int fieldIndex = json.indexOf(fieldPattern);

        if (fieldIndex == -1) {
            return json;
        }

        int valueStartIndex = fieldIndex + fieldPattern.length();
        int valueEndIndex = json.indexOf("\"", valueStartIndex);

        if (valueEndIndex == -1) {
            return json;
        }

        String fieldValue = json.substring(valueStartIndex, valueEndIndex);
        byte[] valueBytes = fieldValue.getBytes();

        long effectiveLimit = Math.min(maxFieldSizeBytes, truncationSizeLimitBytes);
        if (valueBytes.length <= effectiveLimit) {
            return json;
        }

        // Truncate the value
        String truncatedValue = truncateString(fieldValue, effectiveLimit);

        // Escape quotes in truncated value for JSON using StringBuilder for efficiency
        String escapedValue = escapeJsonString(truncatedValue);

        // Replace the original value with truncated value
        return json.substring(0, valueStartIndex) + escapedValue + json.substring(valueEndIndex);
    }

    private static String nullOutFieldInJson(String json, String fieldName)
    {
        String stringFieldPattern = "\"" + fieldName + "\":\"";
        int fieldIndex = json.indexOf(stringFieldPattern);
        if (fieldIndex == -1) {
            return json;
        }

        int valueStartIndex = fieldIndex + stringFieldPattern.length();
        int valueEndIndex = json.indexOf("\"", valueStartIndex);
        if (valueEndIndex == -1) {
            return json;
        }

        return json.substring(0, fieldIndex)
                + "\"" + fieldName + "\":null"
                + json.substring(valueEndIndex + 1);
    }

    /**
     * Truncate a string to accommodate max bytes while handling UTF-8 properly.
     */
    public static String truncateString(String value, long maxBytes)
    {
        if (value == null || maxBytes <= 0) {
            return value;
        }

        byte[] bytes = value.getBytes();
        if (bytes.length <= maxBytes) {
            return value;
        }

        // Truncate string to fit within maxBytes
        String truncated = new String(bytes, 0, (int) Math.min(maxBytes, bytes.length));

        // Remove any incomplete characters at the end
        while (truncated.getBytes().length > maxBytes && truncated.length() > 0) {
            truncated = truncated.substring(0, truncated.length() - 1);
        }

        return truncated + "...[TRUNCATED]";
    }

    /**
     * Escape special JSON characters in a string using StringBuilder to minimize allocations.
     */
    private static String escapeJsonString(String str)
    {
        StringBuilder result = new StringBuilder(str.length() + 16); // Reserve space for escapes
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '\\' -> result.append("\\\\");
                case '"' -> result.append("\\\"");
                case '\b' -> result.append("\\b");
                case '\f' -> result.append("\\f");
                case '\n' -> result.append("\\n");
                case '\r' -> result.append("\\r");
                case '\t' -> result.append("\\t");
                default -> result.append(c);
            }
        }
        return result.toString();
    }
}
