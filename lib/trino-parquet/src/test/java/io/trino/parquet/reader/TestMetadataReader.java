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
package io.trino.parquet.reader;

import com.google.common.io.BaseEncoding;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMetadataReader
{
    private static final Optional<String> NO_CREATED_BY = Optional.empty();

    // com.twitter:parquet-hadoop:jar:1.6.0, parquet.Version.FULL_VERSION
    // The value of this field depends on packaging, so this is the actual value as written by Presto [0.168, 0.214)
    private static final Optional<String> PARQUET_MR = Optional.of("parquet-mr");

    // org.apache.parquet:parquet-common:jar:1.8.1, org.apache.parquet.Version.FULL_VERSION
    // Used by Presto [0.215, 303)
    private static final Optional<String> PARQUET_MR_1_8 = Optional.of("parquet-mr version 1.8.1 (build 4aba4dae7bb0d4edbcf7923ae1339f28fd3f7fcf)");

    // org.apache.parquet:parquet-common:jar:1.10.1, org.apache.parquet.Version.FULL_VERSION
    // Used by Presto [305, ?)
    private static final Optional<String> PARQUET_MR_1_10 = Optional.of("parquet-mr version 1.10.1 (build a89df8f9932b6ef6633d06069e50c9b7970bebd1)");

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsInt32(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("F6FFFFFF"));
        statistics.setMax(fromHex("3AA40000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, INT32, "Test column")))
                .isInstanceOfSatisfying(IntStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), -10);
                    assertEquals(columnStatistics.getMax(), 42042);
                    assertEquals(columnStatistics.genericGetMin(), (Integer) (int) -10);
                    assertEquals(columnStatistics.genericGetMax(), (Integer) 42042);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsInt64(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("F6FFFFFFFFFFFFFF"));
        statistics.setMax(fromHex("3AA4000000000000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, INT64, "Test column")))
                .isInstanceOfSatisfying(LongStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), -10);
                    assertEquals(columnStatistics.getMax(), 42042);
                    assertEquals(columnStatistics.genericGetMin(), (Long) (long) -10L);
                    assertEquals(columnStatistics.genericGetMax(), (Long) 42042L);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsFloat(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("1234ABCD"));
        statistics.setMax(fromHex("12340000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, FLOAT, "Test column")))
                .isInstanceOfSatisfying(FloatStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), -3.59039552E8f);
                    assertEquals(columnStatistics.getMax(), 1.868E-41f);
                    assertEquals(columnStatistics.genericGetMin(), -3.59039552E8f);
                    assertEquals(columnStatistics.genericGetMax(), 1.868E-41f);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsDouble(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("001234ABCD000000"));
        statistics.setMax(fromHex("000000000000E043"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, DOUBLE, "Test column")))
                .isInstanceOfSatisfying(DoubleStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 4.36428250013E-312);
                    assertEquals(columnStatistics.getMax(), 9.223372036854776E18);
                    assertEquals(columnStatistics.genericGetMin(), 4.36428250013E-312);
                    assertEquals(columnStatistics.genericGetMax(), 9.223372036854776E18);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsInt64WithoutNullCount(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setMin(fromHex("F6FFFFFFFFFFFFFF"));
        statistics.setMax(fromHex("3AA4000000000000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, INT64, "Test column")))
                .isInstanceOfSatisfying(LongStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertFalse(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), -1);

                    assertEquals(columnStatistics.getMin(), -10);
                    assertEquals(columnStatistics.getMax(), 42042);
                    assertEquals(columnStatistics.genericGetMin(), (Long) (long) -10L);
                    assertEquals(columnStatistics.genericGetMax(), (Long) 42042L);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsInt64WithoutMin(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMax(fromHex("3AA4000000000000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, INT64, "Test column")))
                .isInstanceOfSatisfying(LongStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 0);
                    assertEquals(columnStatistics.getMax(), 0); // file statistics indicate 42042
                    assertEquals(columnStatistics.genericGetMin(), (Long) 0L);
                    assertEquals(columnStatistics.genericGetMax(), (Long) 0L); // file statistics indicate 42042
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsInt64WithoutMax(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("F6FFFFFFFFFFFFFF"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, INT64, "Test column")))
                .isInstanceOfSatisfying(LongStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 0); // file statistics indicate -10
                    assertEquals(columnStatistics.getMax(), 0);
                    assertEquals(columnStatistics.genericGetMin(), (Long) 0L); // file statistics indicate -10
                    assertEquals(columnStatistics.genericGetMax(), (Long) 0L);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsFloatWithoutMin(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMax(fromHex("12340000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, FLOAT, "Test column")))
                .isInstanceOfSatisfying(FloatStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 0f);
                    assertEquals(columnStatistics.getMax(), 0f);
                    assertEquals(columnStatistics.genericGetMin(), 0f);
                    assertEquals(columnStatistics.genericGetMax(), 0f);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsFloatWithoutMax(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("1234ABCD"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, FLOAT, "Test column")))
                .isInstanceOfSatisfying(FloatStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 0f);
                    assertEquals(columnStatistics.getMax(), 0f);
                    assertEquals(columnStatistics.genericGetMin(), 0f);
                    assertEquals(columnStatistics.genericGetMax(), 0f);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsDoubleWithoutMin(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMax(fromHex("3AA4000000000000"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, DOUBLE, "Test column")))
                .isInstanceOfSatisfying(DoubleStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 0d);
                    assertEquals(columnStatistics.getMax(), 0d);
                    assertEquals(columnStatistics.genericGetMin(), 0d);
                    assertEquals(columnStatistics.genericGetMax(), 0d);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsDoubleWithoutMax(Optional<String> fileCreatedBy)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("F6FFFFFFFFFFFFFF"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, DOUBLE, "Test column")))
                .isInstanceOfSatisfying(DoubleStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin(), 0d);
                    assertEquals(columnStatistics.getMax(), 0d);
                    assertEquals(columnStatistics.genericGetMin(), 0d);
                    assertEquals(columnStatistics.genericGetMax(), 0d);
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsBinary(Optional<String> fileCreatedBy)
    {
        PrimitiveType varbinary = new PrimitiveType(OPTIONAL, BINARY, "Test column");

        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("6162"));
        statistics.setMax(fromHex("DEAD5FC0DE"));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), varbinary))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    // Stats ignored because we did not provide original type and provided min/max for BINARY
                    assertEquals(columnStatistics.getNumNulls(), 13);
                    assertNull(columnStatistics.getMin());
                    assertNull(columnStatistics.getMax());
                    assertNull(columnStatistics.getMinBytes());
                    assertNull(columnStatistics.getMaxBytes());
                    assertNull(columnStatistics.genericGetMin());
                    assertNull(columnStatistics.genericGetMax());
                });

        // Stats written by Parquet after https://issues.apache.org/jira/browse/PARQUET-1025
        statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin_value("a".getBytes(UTF_8));
        statistics.setMax_value("é".getBytes(UTF_8));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), varbinary))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin().getBytes(), new byte[] {'a'});
                    assertEquals(columnStatistics.getMax().getBytes(), new byte[] {(byte) 0xC3, (byte) 0xA9});
                    assertEquals(columnStatistics.getMinBytes(), new byte[] {'a'});
                    assertEquals(columnStatistics.getMaxBytes(), new byte[] {(byte) 0xC3, (byte) 0xA9});
                    assertEquals(columnStatistics.genericGetMin().getBytes(), new byte[] {'a'});
                    assertEquals(columnStatistics.genericGetMax().getBytes(), new byte[] {(byte) 0xC3, (byte) 0xA9});
                });
    }

    /**
     * Stats written potentially before https://issues.apache.org/jira/browse/PARQUET-251
     */
    @Test
    public void testReadStatsBinaryUtf8PotentiallyCorrupted()
    {
        testReadStatsBinaryUtf8OldWriter(NO_CREATED_BY, null, null, null, null);
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR, null, null, null, null);

        testReadStatsBinaryUtf8OldWriter(NO_CREATED_BY, "", "abc", null, null);
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR, "", "abc", null, null);

        testReadStatsBinaryUtf8OldWriter(NO_CREATED_BY, "abc", "def", null, null);
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR, "abc", "def", null, null);

        testReadStatsBinaryUtf8OldWriter(NO_CREATED_BY, "abc", "abc", null, null);
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR, "abc", "abc", null, null);

        testReadStatsBinaryUtf8OldWriter(NO_CREATED_BY, "abcéM", "abcé\u00f7", null, null);
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR, "abcéM", "abcé\u00f7", null, null);
    }

    /**
     * Stats written by Parquet before https://issues.apache.org/jira/browse/PARQUET-1025
     */
    @Test
    public void testReadStatsBinaryUtf8OldWriter()
    {
        // null
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, null, null, null, null);

        // [, bcé]: min is empty, max starts with ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "", "bcé", null, null);

        // [, ébc]: min is empty, max starts with non-ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "", "ébc", null, null);

        // [aa, bé]: no common prefix, first different are both ASCII, min is all ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "aa", "bé", "aa", "c");

        // [abcd, abcdN]: common prefix, not only ASCII, one prefix of the other, last common ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "abcd", "abcdN", "abcd", "abce");

        // [abcé, abcéN]: common prefix, not only ASCII, one prefix of the other, last common non ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "abcé", "abcéN", "abcé", "abd");

        // [abcéM, abcéN]: common prefix, not only ASCII, first different are both ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "abcéM", "abcéN", "abcéM", "abcéO");

        // [abcéMab, abcéNxy]: common prefix, not only ASCII, first different are both ASCII, more characters afterwards
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "abcéMab", "abcéNxy", "abcéMab", "abcéO");

        // [abcéM, abcé\u00f7]: common prefix, not only ASCII, first different are both ASCII, but need to be chopped off (127)
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "abcéM", "abcé\u00f7", "abcéM", "abd");

        // [abc\u007fé, bcd\u007fé]: no common prefix, first different are both ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "abc\u007fé", "bcd\u007fé", "abc\u007f", "c");

        // [é, a]: no common prefix, first different are not both ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "é", "a", null, null);

        // [é, ê]: no common prefix, first different are both not ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "é", "ê", null, null);

        // [aé, aé]: min = max (common prefix, first different are both not ASCII)
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "aé", "aé", "aé", "aé");

        // [aé, bé]: no common prefix, first different are both ASCII
        testReadStatsBinaryUtf8OldWriter(PARQUET_MR_1_8, "aé", "bé", "a", "c");
    }

    private void testReadStatsBinaryUtf8OldWriter(Optional<String> fileCreatedBy, String min, String max, String expectedMin, String expectedMax)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        if (min != null) {
            statistics.setMin(min.getBytes(UTF_8));
        }
        if (max != null) {
            statistics.setMax(max.getBytes(UTF_8));
        }
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("Test column")))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    byte[] expectedMinBytes = expectedMin != null ? expectedMin.getBytes(UTF_8) : null;
                    assertThat(columnStatistics.getMinBytes()).isEqualTo(expectedMinBytes);
                    if (expectedMinBytes != null) {
                        assertThat(columnStatistics.getMin().getBytes()).isEqualTo(expectedMinBytes);
                        assertThat(columnStatistics.genericGetMin().getBytes()).isEqualTo(expectedMinBytes);
                    }
                    else {
                        assertNull(columnStatistics.getMin());
                        assertNull(columnStatistics.genericGetMin());
                    }

                    byte[] expectedMaxBytes = expectedMax != null ? expectedMax.getBytes(UTF_8) : null;
                    assertThat(columnStatistics.getMaxBytes()).isEqualTo(expectedMaxBytes);
                    if (expectedMaxBytes != null) {
                        assertThat(columnStatistics.getMax().getBytes()).isEqualTo(expectedMaxBytes);
                        assertThat(columnStatistics.genericGetMax().getBytes()).isEqualTo(expectedMaxBytes);
                    }
                    else {
                        assertNull(columnStatistics.getMax());
                        assertNull(columnStatistics.genericGetMax());
                    }
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsBinaryUtf8(Optional<String> fileCreatedBy)
    {
        PrimitiveType varchar = Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("Test column");
        Statistics statistics;

        // Stats written by Parquet after https://issues.apache.org/jira/browse/PARQUET-1025
        statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin_value("a".getBytes(UTF_8));
        statistics.setMax_value("é".getBytes(UTF_8));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), varchar))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    assertFalse(columnStatistics.isEmpty());

                    assertTrue(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), 13);

                    assertEquals(columnStatistics.getMin().getBytes(), new byte[] {'a'});
                    assertEquals(columnStatistics.getMax().getBytes(), new byte[] {(byte) 0xC3, (byte) 0xA9});
                    assertEquals(columnStatistics.getMinBytes(), new byte[] {'a'});
                    assertEquals(columnStatistics.getMaxBytes(), new byte[] {(byte) 0xC3, (byte) 0xA9});
                    assertEquals(columnStatistics.genericGetMin().getBytes(), new byte[] {'a'});
                    assertEquals(columnStatistics.genericGetMax().getBytes(), new byte[] {(byte) 0xC3, (byte) 0xA9});
                });
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadNullStats(Optional<String> fileCreatedBy)
    {
        // integer
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.empty(), new PrimitiveType(OPTIONAL, INT32, "Test column")))
                .isInstanceOfSatisfying(
                        IntStatistics.class,
                        columnStatistics -> assertTrue(columnStatistics.isEmpty()));

        // bigint
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.empty(), new PrimitiveType(OPTIONAL, INT64, "Test column")))
                .isInstanceOfSatisfying(
                        LongStatistics.class,
                        columnStatistics -> assertTrue(columnStatistics.isEmpty()));

        // varchar
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.empty(), Types.optional(BINARY).as(LogicalTypeAnnotation.stringType()).named("Test column")))
                .isInstanceOfSatisfying(
                        BinaryStatistics.class,
                        columnStatistics -> assertTrue(columnStatistics.isEmpty()));

        // varbinary
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.empty(), new PrimitiveType(OPTIONAL, BINARY, "Test column")))
                .isInstanceOfSatisfying(
                        BinaryStatistics.class,
                        columnStatistics -> assertTrue(columnStatistics.isEmpty()));
    }

    @DataProvider
    public Object[][] allCreatedBy()
    {
        return new Object[][] {
                {NO_CREATED_BY},
                {PARQUET_MR},
                {PARQUET_MR_1_8},
                {PARQUET_MR_1_10},
        };
    }

    private static byte[] fromHex(String hex)
    {
        return BaseEncoding.base16().decode(hex);
    }
}
