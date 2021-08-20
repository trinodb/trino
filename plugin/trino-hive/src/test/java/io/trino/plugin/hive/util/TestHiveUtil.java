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
package io.trino.plugin.hive.util;

import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.type.TimeZoneKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Properties;

import static io.airlift.testing.Assertions.assertInstanceOf;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTableProperties.PARQUET_BLOOM_FILTER_COLUMNS;
import static io.trino.plugin.hive.HiveTableProperties.PARQUET_BLOOM_FILTER_FPP;
import static io.trino.plugin.hive.util.HiveUtil.getDeserializer;
import static io.trino.plugin.hive.util.HiveUtil.getInputFormat;
import static io.trino.plugin.hive.util.HiveUtil.getParquetWriterOptions;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveTimestamp;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_CLASS;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestHiveUtil
{
    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123, DateTimeZone.UTC);
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss"), unixTime(time, 0));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.S"), unixTime(time, 1));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSS"), unixTime(time, 3));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS"), unixTime(time, 6));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), unixTime(time, 7));
    }

    @Test
    public void testGetThriftDeserializer()
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, ThriftDeserializer.class.getName());
        schema.setProperty(SERIALIZATION_CLASS, IntString.class.getName());
        schema.setProperty(SERIALIZATION_FORMAT, TBinaryProtocol.class.getName());

        assertInstanceOf(getDeserializer(new Configuration(false), schema), ThriftDeserializer.class);
    }

    @Test
    public void testToPartitionValues()
            throws MetaException
    {
        assertToPartitionValues("ds=2015-12-30/event_type=QueryCompletion");
        assertToPartitionValues("ds=2015-12-30");
        assertToPartitionValues("a=1/b=2/c=3");
        assertToPartitionValues("a=1");
        assertToPartitionValues("pk=!@%23$%25%5E&%2A()%2F%3D");
        assertToPartitionValues("pk=__HIVE_DEFAULT_PARTITION__");
    }

    @Test
    public void testGetInputFormat()
    {
        Configuration configuration = new Configuration(false);

        Properties avroSymlinkSchema = new Properties();
        avroSymlinkSchema.setProperty(FILE_INPUT_FORMAT, SymlinkTextInputFormat.class.getName());
        avroSymlinkSchema.setProperty(SERIALIZATION_LIB, AVRO.getSerDe());
        assertInstanceOf(getInputFormat(configuration, avroSymlinkSchema, false), SymlinkTextInputFormat.class);
        assertInstanceOf(getInputFormat(configuration, avroSymlinkSchema, true), AvroContainerInputFormat.class);

        Properties parquetSymlinkSchema = new Properties();
        parquetSymlinkSchema.setProperty(FILE_INPUT_FORMAT, SymlinkTextInputFormat.class.getName());
        parquetSymlinkSchema.setProperty(SERIALIZATION_LIB, PARQUET.getSerDe());
        assertInstanceOf(getInputFormat(configuration, parquetSymlinkSchema, false), SymlinkTextInputFormat.class);
        assertInstanceOf(getInputFormat(configuration, parquetSymlinkSchema, true), MapredParquetInputFormat.class);

        Properties parquetSchema = new Properties();
        parquetSchema.setProperty(FILE_INPUT_FORMAT, PARQUET.getInputFormat());
        assertInstanceOf(getInputFormat(configuration, parquetSchema, false), MapredParquetInputFormat.class);
        assertInstanceOf(getInputFormat(configuration, parquetSchema, true), MapredParquetInputFormat.class);

        Properties legacyParquetSchema = new Properties();
        legacyParquetSchema.setProperty(FILE_INPUT_FORMAT, "parquet.hive.MapredParquetInputFormat");
        assertInstanceOf(getInputFormat(configuration, legacyParquetSchema, false), MapredParquetInputFormat.class);
        assertInstanceOf(getInputFormat(configuration, legacyParquetSchema, true), MapredParquetInputFormat.class);
    }

    private static void assertToPartitionValues(String partitionName)
            throws MetaException
    {
        List<String> actual = toPartitionValues(partitionName);
        AbstractList<String> expected = new ArrayList<>();
        actual.forEach(s -> expected.add(null));
        Warehouse.makeValsFromName(partitionName, expected);
        assertEquals(actual, expected);
    }

    private static long parse(DateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time));
    }

    private static long unixTime(DateTime time, int factionalDigits)
    {
        int factor = (int) Math.pow(10, Math.max(0, 3 - factionalDigits));
        return (time.getMillis() / factor) * factor * MICROSECONDS_PER_MILLISECOND;
    }

    public static DateTimeZone nonDefaultTimeZone()
    {
        String defaultId = DateTimeZone.getDefault().getID();
        for (String id : DateTimeZone.getAvailableIDs()) {
            if (!id.equals(defaultId)) {
                DateTimeZone zone = DateTimeZone.forID(id);
                if (zone.getStandardOffset(0) != 0) {
                    return zone;
                }
            }
        }
        throw new IllegalStateException("no non-default timezone");
    }

    @Test
    public void testGetParquetWriterOptions()
    {
        Properties tableProperties = new Properties();
        TestingConnectorSession session = new TestingConnectorSession();

        assertThat(getParquetWriterOptions(session, tableProperties))
                .isEqualToComparingFieldByFieldRecursively(ParquetWriterOptions.builder()
                        .setMaxPageSize(TestingConnectorSession.PARQUET_WRITER_PAGE_SIZE)
                        .setMaxBlockSize(TestingConnectorSession.PARQUET_WRITER_BLOCK_SIZE)
                        .build());

        tableProperties.setProperty(PARQUET_BLOOM_FILTER_COLUMNS, "col_1, col_2, col_3");
        assertThatThrownBy(() -> getParquetWriterOptions(session, tableProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Parquet bloom filter false positive probability is missing");

        tableProperties.setProperty(PARQUET_BLOOM_FILTER_FPP, "-1");
        assertThatThrownBy(() -> getParquetWriterOptions(session, tableProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("False positive probability should be between 0 and 1");

        tableProperties.setProperty(PARQUET_BLOOM_FILTER_FPP, "0.05");
        assertThat(getParquetWriterOptions(session, tableProperties))
                .isEqualToComparingFieldByFieldRecursively(ParquetWriterOptions.builder()
                        .setMaxPageSize(TestingConnectorSession.PARQUET_WRITER_PAGE_SIZE)
                        .setMaxBlockSize(TestingConnectorSession.PARQUET_WRITER_BLOCK_SIZE)
                        .setBloomFilterFpp(0.05)
                        .setBloomFilterColumns(ImmutableSet.of("col_1", "col_2", "col_3"))
                        .build());

        tableProperties.setProperty(PARQUET_BLOOM_FILTER_FPP, "invalid number.7");
        assertThatThrownBy(() -> getParquetWriterOptions(session, tableProperties))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'invalid number.7' is an invalid bloom filter false positive probability");
    }

    private static class TestingConnectorSession
            implements ConnectorSession
    {
        public static final DataSize PARQUET_WRITER_PAGE_SIZE = DataSize.ofBytes(800);
        public static final DataSize PARQUET_WRITER_BLOCK_SIZE = DataSize.ofBytes(1024 * 1024);

        @Override
        public String getQueryId()
        {
            return "";
        }

        @Override
        public Optional<String> getSource()
        {
            return Optional.empty();
        }

        @Override
        public ConnectorIdentity getIdentity()
        {
            return null;
        }

        @Override
        public TimeZoneKey getTimeZoneKey()
        {
            return null;
        }

        @Override
        public Locale getLocale()
        {
            return null;
        }

        @Override
        public Optional<String> getTraceToken()
        {
            return Optional.empty();
        }

        @Override
        public Instant getStart()
        {
            return null;
        }

        @Override
        public <T> T getProperty(String propertyName, Class<T> type)
        {
            switch (propertyName) {
                case "parquet_writer_page_size":
                    return (T) PARQUET_WRITER_PAGE_SIZE;
                case "parquet_writer_block_size":
                    return (T) PARQUET_WRITER_BLOCK_SIZE;
                default:
                    return (T) DataSize.ofBytes(0);
            }
        }
    }
}
