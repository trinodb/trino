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
package io.trino.plugin.hive.s3select;

import com.google.common.collect.ImmutableSet;
import io.trino.hive.formats.compression.CompressionKind;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSession;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static io.trino.plugin.hive.HiveMetadata.SKIP_FOOTER_COUNT_KEY;
import static io.trino.plugin.hive.HiveMetadata.SKIP_HEADER_COUNT_KEY;
import static io.trino.plugin.hive.HiveSessionProperties.isS3SelectPushdownEnabled;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.util.HiveClassNames.TEXT_INPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializerClassName;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormatName;
import static java.util.Objects.requireNonNull;

/**
 * S3SelectPushdown uses Amazon S3 Select to push down queries to Amazon S3. This allows Presto to retrieve only a
 * subset of data rather than retrieving the full S3 object thus improving Presto query performance.
 */
public final class S3SelectPushdown
{
    private static final Set<String> SUPPORTED_S3_PREFIXES = ImmutableSet.of("s3://", "s3a://", "s3n://");

    /*
     * Double and Real Types lose precision. Thus, they are not pushed down to S3.
     * Correctness problems have also been observed with Decimal columns.
     *
     * When S3 select support was added, Trino did not properly implement TIMESTAMP semantic. This was fixed in 2020, and TIMESTAMPS may be supportable now
     * (https://github.com/trinodb/trino/issues/10962). Pushing down timestamps to s3select maybe still be problematic due to ION SQL comparing timestamps
     * using precision.  This means timestamps with different precisions are not equal even actually they present the same instant of time.
     */
    private static final Set<String> SUPPORTED_COLUMN_TYPES = ImmutableSet.of(
            "boolean",
            "int",
            "tinyint",
            "smallint",
            "bigint",
            "string",
            "date");

    private S3SelectPushdown() {}

    private static boolean isSerDeSupported(Properties schema)
    {
        String serdeName = getDeserializerClassName(schema);
        return S3SelectSerDeDataTypeMapper.doesSerDeExist(serdeName);
    }

    private static boolean isInputFormatSupported(Properties schema)
    {
        if (isTextInputFormat(schema)) {
            if (!Objects.equals(schema.getProperty(SKIP_HEADER_COUNT_KEY, "0"), "0")) {
                // S3 Select supports skipping one line of headers, but it was returning incorrect results for trino-hive-hadoop2/conf/files/test_table_with_header.csv.gz
                // TODO https://github.com/trinodb/trino/issues/2349
                return false;
            }

            // S3 Select does not support skipping footers
            return Objects.equals(schema.getProperty(SKIP_FOOTER_COUNT_KEY, "0"), "0");
        }

        return false;
    }

    public static boolean isCompressionCodecSupported(Properties schema, String path)
    {
        if (isTextInputFormat(schema)) {
            // S3 Select supports the following formats: uncompressed, GZIP and BZIP2.
            return CompressionKind.forFile(path)
                    .map(kind -> kind == CompressionKind.GZIP || kind == CompressionKind.BZIP2)
                    .orElse(true);
        }

        return false;
    }

    public static boolean isSplittable(boolean s3SelectPushdownEnabled, Properties schema, String path)
    {
        if (!s3SelectPushdownEnabled) {
            return true;
        }

        // S3 Select supports splitting uncompressed files
        if (isTextInputFormat(schema) && CompressionKind.forFile(path).isEmpty()) {
            return isSerDeSupported(schema);
        }

        return false;
    }

    private static boolean isTextInputFormat(Properties schema)
    {
        return TEXT_INPUT_FORMAT_CLASS.equals(getInputFormatName(schema).orElse(null));
    }

    private static boolean areColumnTypesSupported(List<Column> columns)
    {
        requireNonNull(columns, "columns is null");

        if (columns.isEmpty()) {
            return false;
        }

        for (Column column : columns) {
            if (!SUPPORTED_COLUMN_TYPES.contains(column.getType().getHiveTypeName().toString())) {
                return false;
            }
        }

        return true;
    }

    private static boolean isS3Storage(String path)
    {
        return SUPPORTED_S3_PREFIXES.stream().anyMatch(path::startsWith);
    }

    public static boolean shouldEnablePushdownForTable(ConnectorSession session, Table table, String path, Optional<Partition> optionalPartition)
    {
        if (!isS3SelectPushdownEnabled(session)) {
            return false;
        }

        if (path == null) {
            return false;
        }

        // Hive table partitions could be on different storages,
        // as a result, we have to check each individual optionalPartition
        Properties schema = optionalPartition
                .map(partition -> getHiveSchema(partition, table))
                .orElseGet(() -> getHiveSchema(table));
        return shouldEnablePushdownForTable(table, path, schema);
    }

    private static boolean shouldEnablePushdownForTable(Table table, String path, Properties schema)
    {
        return isS3Storage(path) &&
                isSerDeSupported(schema) &&
                isInputFormatSupported(schema) &&
                areColumnTypesSupported(table.getDataColumns());
    }
}
