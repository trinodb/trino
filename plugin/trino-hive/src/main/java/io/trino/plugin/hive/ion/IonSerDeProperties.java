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
package io.trino.plugin.hive.ion;

import com.amazon.ion.IonWriter;
import com.amazon.ion.system.IonBinaryWriterBuilder;
import com.amazon.ion.system.IonTextWriterBuilder;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.hive.formats.ion.IonDecoderConfig;
import io.trino.spi.TrinoException;

import java.io.OutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;

public final class IonSerDeProperties
{
    private static final Logger log = Logger.get(IonSerDeProperties.class);

    // Reader properties
    public static final String STRICT_PATH_TYPING_PROPERTY = "ion.path_extractor.strict";
    public static final String STRICT_PATH_TYPING_DEFAULT = "false";
    public static final String PATH_EXTRACTOR_PROPERTY = "ion.(\\w+).path_extractor";
    public static final String PATH_EXTRACTION_CASE_SENSITIVITY = "ion.path_extractor.case_sensitive";
    public static final String PATH_EXTRACTION_CASE_SENSITIVITY_DEFAULT = "false";
    private static final Pattern pathExtractorPattern = Pattern.compile(PATH_EXTRACTOR_PROPERTY);

    // unimplemented reader properties
    public static final String FAIL_ON_OVERFLOW_PROPERTY = "ion.fail_on_overflow";
    public static final String FAIL_ON_OVERFLOW_PROPERTY_DEFAULT = "true";
    public static final String FAIL_ON_OVERFLOW_COLUMN_PROPERTY = "ion.\\w+.fail_on_overflow";
    public static final String IGNORE_MALFORMED = "ion.ignore_malformed";
    public static final String IGNORE_MALFORMED_DEFAULT = "false";

    // Writer properties
    public static final String ION_ENCODING_PROPERTY = "ion.encoding";
    public static final String TEXT_ENCODING = "text";
    public static final String BINARY_ENCODING = "binary";

    // unimplemented writer properties
    public static final String ION_TIMESTAMP_OFFSET_PROPERTY = "ion.timestamp.serialization_offset";
    public static final String ION_TIMESTAMP_OFFSET_DEFAULT = "Z";
    public static final String ION_SERIALIZE_NULL_AS_PROPERTY = "ion.serialize_null";
    public static final String ION_SERIALIZE_NULL_AS_DEFAULT = "OMIT";
    public static final String ION_SERIALIZE_NULL_AS_COLUMN_PROPERTY = "ion.\\w+.serialize_as";

    private static final Pattern unsupportedPropertiesRegex = Pattern.compile(
            ION_SERIALIZE_NULL_AS_COLUMN_PROPERTY + "|" + FAIL_ON_OVERFLOW_COLUMN_PROPERTY);

    private static final Map<String, String> defaultOnlyProperties = Map.of(
            // reader properties
            FAIL_ON_OVERFLOW_PROPERTY, FAIL_ON_OVERFLOW_PROPERTY_DEFAULT,
            IGNORE_MALFORMED, IGNORE_MALFORMED_DEFAULT,

            // writer properties
            ION_TIMESTAMP_OFFSET_PROPERTY, ION_TIMESTAMP_OFFSET_DEFAULT,
            ION_SERIALIZE_NULL_AS_PROPERTY, ION_SERIALIZE_NULL_AS_DEFAULT);

    private IonSerDeProperties() {}

    public static IonDecoderConfig decoderConfigFor(Map<String, String> propertiesMap)
    {
        ImmutableMap.Builder<String, String> extractionsBuilder = ImmutableMap.builder();

        for (Map.Entry<String, String> property : propertiesMap.entrySet()) {
            Matcher matcher = pathExtractorPattern.matcher(property.getKey());
            if (matcher.matches()) {
                extractionsBuilder.put(matcher.group(1), property.getValue());
            }
        }

        boolean strictTyping = Boolean.parseBoolean(
                propertiesMap.getOrDefault(STRICT_PATH_TYPING_PROPERTY, STRICT_PATH_TYPING_DEFAULT));
        boolean caseSensitive = Boolean.parseBoolean(
                propertiesMap.getOrDefault(PATH_EXTRACTION_CASE_SENSITIVITY, PATH_EXTRACTION_CASE_SENSITIVITY_DEFAULT));

        return new IonDecoderConfig(extractionsBuilder.buildOrThrow(), strictTyping, caseSensitive);
    }

    public enum IonEncoding
    {
        BINARY {
            @Override
            public IonWriter createWriter(OutputStream outputStream)
            {
                return IonBinaryWriterBuilder.standard().build(outputStream);
            }
        },

        TEXT {
            @Override
            public IonWriter createWriter(OutputStream outputStream)
            {
                return IonTextWriterBuilder.minimal().build(outputStream);
            }
        };

        public abstract IonWriter createWriter(OutputStream outputStream);
    }

    public static IonEncoding getIonEncoding(Map<String, String> schema)
    {
        String encodingStr = schema.getOrDefault(ION_ENCODING_PROPERTY, BINARY_ENCODING);
        return switch (encodingStr.toLowerCase(Locale.ROOT)) {
            case TEXT_ENCODING -> IonEncoding.TEXT;
            case BINARY_ENCODING -> IonEncoding.BINARY;
            default -> throw new TrinoException(HIVE_UNSUPPORTED_FORMAT,
                    "Unsupported Ion encoding format: " + encodingStr);
        };
    }

    /**
     * Checks if all of the properties starting with "ion." are supported.
     * Throws TrinoException if any are unsupported.
     */
    public static void validatePropertySupport(Map<String, String> properties)
    {
        List<String> errors = new LinkedList<>();
        for (Map.Entry<String, String> entry : properties.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            if (!key.startsWith("ion.")) {
                continue;
            }
            String defaultValue = defaultOnlyProperties.get(key);
            if ((defaultValue != null && !defaultValue.equals(value))
                    || unsupportedPropertiesRegex.matcher(key).matches()) {
                log.error("Ion Table contains unsupported SerDe property: %s => %s", key, value);
                errors.add(String.format("%s = %s", key, value));
            }
        }
        if (!errors.isEmpty()) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Ion Table contains unsupported SerDe properties: " + String.join(", ", errors));
        }
    }
}
