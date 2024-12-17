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
package io.trino.plugin.kudu.properties;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.session.PropertyMetadata;
import org.apache.kudu.ColumnSchema;

import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;
import static io.trino.spi.session.PropertyMetadata.stringProperty;
import static java.util.Objects.requireNonNull;

public final class KuduColumnProperties
{
    public static final String PRIMARY_KEY = "primary_key";
    public static final String NULLABLE = "nullable";
    public static final String ENCODING = "encoding";
    public static final String COMPRESSION = "compression";

    private final List<PropertyMetadata<?>> columnProperties;

    @Inject
    public KuduColumnProperties()
    {
        columnProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        PRIMARY_KEY,
                        "If column belongs to primary key",
                        false,
                        false))
                .add(booleanProperty(
                        NULLABLE,
                        "If column can be set to null",
                        false,
                        false))
                .add(stringProperty(
                        ENCODING,
                        "Optional specification of the column encoding. Otherwise default encoding is applied.",
                        null,
                        false))
                .add(stringProperty(
                        COMPRESSION,
                        "Optional specification of the column compression. Otherwise default compression is applied.",
                        null,
                        false))
                .build();
    }

    public List<PropertyMetadata<?>> getColumnProperties()
    {
        return columnProperties;
    }

    public static ColumnDesign getColumnDesign(Map<String, Object> columnProperties)
    {
        requireNonNull(columnProperties);
        if (columnProperties.isEmpty()) {
            return ColumnDesign.DEFAULT;
        }

        ColumnDesign design = new ColumnDesign();
        Boolean key = (Boolean) columnProperties.get(PRIMARY_KEY);
        if (key != null) {
            design.setPrimaryKey(key);
        }

        Boolean nullable = (Boolean) columnProperties.get(NULLABLE);
        if (nullable != null) {
            design.setNullable(nullable);
        }

        String encoding = (String) columnProperties.get(ENCODING);
        if (encoding != null) {
            design.setEncoding(encoding);
        }

        String compression = (String) columnProperties.get(COMPRESSION);
        if (compression != null) {
            design.setCompression(compression);
        }
        return design;
    }

    public static ColumnSchema.CompressionAlgorithm lookupCompression(String compression)
    {
        return switch (compression.toLowerCase(Locale.ENGLISH)) {
            case "default", "default_compression" -> ColumnSchema.CompressionAlgorithm.DEFAULT_COMPRESSION;
            case "no", "no_compression" -> ColumnSchema.CompressionAlgorithm.NO_COMPRESSION;
            case "lz4" -> ColumnSchema.CompressionAlgorithm.LZ4;
            case "snappy" -> ColumnSchema.CompressionAlgorithm.SNAPPY;
            case "zlib" -> ColumnSchema.CompressionAlgorithm.ZLIB;
            default -> throw new IllegalArgumentException();
        };
    }

    public static String lookupCompressionString(ColumnSchema.CompressionAlgorithm algorithm)
    {
        return switch (algorithm) {
            case DEFAULT_COMPRESSION -> "default";
            case NO_COMPRESSION -> "no";
            case LZ4 -> "lz4";
            case SNAPPY -> "snappy";
            case ZLIB -> "zlib";
            default -> "unknown";
        };
    }

    public static ColumnSchema.Encoding lookupEncoding(String encoding)
    {
        return switch (encoding.toLowerCase(Locale.ENGLISH)) {
            case "auto", "auto_encoding" -> ColumnSchema.Encoding.AUTO_ENCODING;
            case "bitshuffle", "bit_shuffle" -> ColumnSchema.Encoding.BIT_SHUFFLE;
            case "dictionary", "dict_encoding" -> ColumnSchema.Encoding.DICT_ENCODING;
            case "plain", "plain_encoding" -> ColumnSchema.Encoding.PLAIN_ENCODING;
            case "prefix", "prefix_encoding" -> ColumnSchema.Encoding.PREFIX_ENCODING;
            case "runlength", "run_length", "run length", "rle" -> ColumnSchema.Encoding.RLE;
            case "group_varint" -> ColumnSchema.Encoding.GROUP_VARINT;
            default -> throw new IllegalArgumentException();
        };
    }

    public static String lookupEncodingString(ColumnSchema.Encoding encoding)
    {
        return switch (encoding) {
            case AUTO_ENCODING -> "auto";
            case BIT_SHUFFLE -> "bitshuffle";
            case DICT_ENCODING -> "dictionary";
            case PLAIN_ENCODING -> "plain";
            case PREFIX_ENCODING -> "prefix";
            case RLE -> "runlength";
            case GROUP_VARINT -> "group_varint";
            default -> "unknown";
        };
    }
}
