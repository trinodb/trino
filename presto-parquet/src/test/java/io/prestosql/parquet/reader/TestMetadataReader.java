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
package io.prestosql.parquet.reader;

import com.google.common.io.BaseEncoding;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.format.Statistics;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestMetadataReader
{
    private static final Optional<String> NO_CREATED_BY = Optional.empty();

    // com.twitter:parquet-hadoop:jar:1.6.0, parquet.Version.FULL_VERSION
    // The value of this field depends on packaging, so this is the actual value as written by Presto [0.168, 0.214)
    private static final Optional<String> PARQUET_MR = Optional.of("parquet-mr");

    // org.apache.parquet:parquet-common:jar:1.10.1, org.apache.parquet.Version.FULL_VERSION
    // Used by Presto [0.215, 303)
    private static final Optional<String> PARQUET_MR_1_8 = Optional.of("parquet-mr version 1.8.2 (build c6522788629e590a53eb79874b95f6c3ff11f16c)");

    // org.apache.parquet:parquet-common:jar:1.10.1, org.apache.parquet.Version.FULL_VERSION
    // Used by Presto [303, ?)
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
                    assertEquals(columnStatistics.getNumNulls(), 13);
                    assertEquals(columnStatistics.getMin(), -10);
                    assertEquals(columnStatistics.getMax(), 42042);
                    assertEquals(columnStatistics.genericGetMin(), (Long) (long) -10L);
                    assertEquals(columnStatistics.genericGetMax(), (Long) 42042L);
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
                    assertFalse(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), -1);
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
     * Stats written by Parquet before https://issues.apache.org/jira/browse/PARQUET-1025
     */
    @Test(dataProvider = "testReadStatsBinaryUtf8OldWriterDataProvider")
    public void testReadStatsBinaryUtf8OldWriter(Optional<String> fileCreatedBy, int nullCount, byte[] min, byte[] max, int expectedNullCount, byte[] expectedMin, byte[] expectedMax)
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(nullCount);
        statistics.setMin(min);
        statistics.setMax(max);
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), new PrimitiveType(OPTIONAL, BINARY, "Test column", OriginalType.UTF8)))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    assertEquals(columnStatistics.getNumNulls(), expectedNullCount);

                    assertEquals(columnStatistics.getMinBytes(), expectedMin);
                    if (expectedMin != null) {
                        assertEquals(columnStatistics.getMin().getBytes(), expectedMin);
                        assertEquals(columnStatistics.genericGetMin().getBytes(), expectedMin);
                    }
                    else {
                        assertNull(columnStatistics.getMin());
                        assertNull(columnStatistics.genericGetMin());
                    }

                    assertEquals(columnStatistics.getMaxBytes(), expectedMax);
                    if (expectedMax != null) {
                        assertEquals(columnStatistics.getMax().getBytes(), expectedMax);
                        assertEquals(columnStatistics.genericGetMax().getBytes(), expectedMax);
                    }
                    else {
                        assertNull(columnStatistics.getMax());
                        assertNull(columnStatistics.genericGetMax());
                    }
                });
    }

    @DataProvider
    public Object[][] testReadStatsBinaryUtf8OldWriterDataProvider()
    {
        return new Object[][] {
                // [aa, bé]
                {NO_CREATED_BY, 13, "aa".getBytes(UTF_8), "bé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR, 13, "aa".getBytes(UTF_8), "bé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_8, 13, "aa".getBytes(UTF_8), "bé".getBytes(UTF_8), 13, "aa".getBytes(UTF_8), "c".getBytes(UTF_8)},
                {PARQUET_MR_1_10, 13, "aa".getBytes(UTF_8), "bé".getBytes(UTF_8), 13, "aa".getBytes(UTF_8), "c".getBytes(UTF_8)}, // however, 1.10 won't fill old min/max

                // [abc\u007fé, bcd\u007fé]; \u007f is retained in min value, but removed from max
                {NO_CREATED_BY, 13, "abc\u007fé".getBytes(UTF_8), "bcd\u007fé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR, 13, "abc\u007fé".getBytes(UTF_8), "bcd\u007fé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_8, 13, "abc\u007fé".getBytes(UTF_8), "bcd\u007fé".getBytes(UTF_8), 13, "abc\u007f".getBytes(UTF_8), "bce".getBytes(UTF_8)},
                // however, 1.10 won't fill old min/max
                {PARQUET_MR_1_10, 13, "abc\u007fé".getBytes(UTF_8), "bcd\u007fé".getBytes(UTF_8), 13, "abc\u007f".getBytes(UTF_8), "bce".getBytes(UTF_8)},

                // [é, a] or [a, é]
                {NO_CREATED_BY, 13, "é".getBytes(UTF_8), "a".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR, 13, "é".getBytes(UTF_8), "a".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_8, 13, "é".getBytes(UTF_8), "a".getBytes(UTF_8), 13, new byte[0], "b".getBytes(UTF_8)},
                {PARQUET_MR_1_10, 13, "a".getBytes(UTF_8), "é".getBytes(UTF_8), -1, null, null}, // however, 1.10 won't fill old min/max

                // [é, ê]; both, before PARQUET-1025 and after than, Parquet writer would order them this way
                {NO_CREATED_BY, 13, "é".getBytes(UTF_8), "ê".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR, 13, "é".getBytes(UTF_8), "ê".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_8, 13, "é".getBytes(UTF_8), "ê".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_10, 13, "é".getBytes(UTF_8), "ê".getBytes(UTF_8), -1, null, null}, // however, 1.10 won't fill old min/max

                // [aé, aé]
                {NO_CREATED_BY, 13, "aé".getBytes(UTF_8), "aé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR, 13, "aé".getBytes(UTF_8), "aé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_8, 13, "aé".getBytes(UTF_8), "aé".getBytes(UTF_8), 13, "aé".getBytes(UTF_8), "aé".getBytes(UTF_8)},
                {PARQUET_MR_1_10, 13, "aé".getBytes(UTF_8), "aé".getBytes(UTF_8), 13, "aé".getBytes(UTF_8), "aé".getBytes(UTF_8)}, // however, 1.10 won't fill old min/max

                // [aé, bé]
                {NO_CREATED_BY, 13, "aé".getBytes(UTF_8), "bé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR, 13, "aé".getBytes(UTF_8), "bé".getBytes(UTF_8), -1, null, null},
                {PARQUET_MR_1_8, 13, "aé".getBytes(UTF_8), "bé".getBytes(UTF_8), 13, "a".getBytes(UTF_8), "c".getBytes(UTF_8)},
                {PARQUET_MR_1_10, 13, "aé".getBytes(UTF_8), "bé".getBytes(UTF_8), 13, "a".getBytes(UTF_8), "c".getBytes(UTF_8)}, // however, 1.10 won't fill old min/max
        };
    }

    @Test(dataProvider = "allCreatedBy")
    public void testReadStatsBinaryUtf8(Optional<String> fileCreatedBy)
    {
        PrimitiveType varchar = new PrimitiveType(OPTIONAL, BINARY, "Test column", OriginalType.UTF8);
        Statistics statistics;

        // Stats written by Parquet after https://issues.apache.org/jira/browse/PARQUET-1025
        statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin_value("a".getBytes(UTF_8));
        statistics.setMax_value("é".getBytes(UTF_8));
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.of(statistics), varchar))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
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
        assertThat(MetadataReader.readStats(fileCreatedBy, Optional.empty(), new PrimitiveType(OPTIONAL, BINARY, "Test column", OriginalType.UTF8)))
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
