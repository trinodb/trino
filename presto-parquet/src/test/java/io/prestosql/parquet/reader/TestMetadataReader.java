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
import org.testng.annotations.Test;

import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;

public class TestMetadataReader
{
    @Test
    public void testReadStatsInt32()
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("F6FFFFFF"));
        statistics.setMax(fromHex("3AA40000"));
        assertThat(MetadataReader.readStats(statistics, new PrimitiveType(OPTIONAL, INT32, "Test column")))
                .isInstanceOfSatisfying(IntStatistics.class, columnStatistics -> {
                    assertEquals(columnStatistics.getNumNulls(), 13);
                    assertEquals(columnStatistics.getMin(), -10);
                    assertEquals(columnStatistics.getMax(), 42042);
                    assertEquals(columnStatistics.genericGetMin(), (Integer) (int) -10);
                    assertEquals(columnStatistics.genericGetMax(), (Integer) 42042);
                });
    }

    @Test
    public void testReadStatsInt64()
    {
        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("F6FFFFFFFFFFFFFF"));
        statistics.setMax(fromHex("3AA4000000000000"));
        assertThat(MetadataReader.readStats(statistics, new PrimitiveType(OPTIONAL, INT64, "Test column")))
                .isInstanceOfSatisfying(LongStatistics.class, columnStatistics -> {
                    assertEquals(columnStatistics.getNumNulls(), 13);
                    assertEquals(columnStatistics.getMin(), -10);
                    assertEquals(columnStatistics.getMax(), 42042);
                    assertEquals(columnStatistics.genericGetMin(), (Long) (long) -10L);
                    assertEquals(columnStatistics.genericGetMax(), (Long) 42042L);
                });
    }

    @Test
    public void testReadStatsBinary()
    {
        PrimitiveType varbinary = new PrimitiveType(OPTIONAL, BINARY, "Test column");

        Statistics statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin(fromHex("6162"));
        statistics.setMax(fromHex("DEAD5FC0DE"));
        assertThat(MetadataReader.readStats(statistics, varbinary))
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
        assertThat(MetadataReader.readStats(statistics, varbinary))
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

    @Test
    public void testReadStatsBinaryUtf8()
    {
        PrimitiveType varchar = new PrimitiveType(OPTIONAL, BINARY, "Test column", OriginalType.UTF8);
        Statistics statistics;

        // Stats written by Parquet before https://issues.apache.org/jira/browse/PARQUET-1025
        statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin("aa".getBytes(UTF_8));
        statistics.setMax("bé".getBytes(UTF_8));
        assertThat(MetadataReader.readStats(statistics, varchar))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    // Stats ignored because we provided min/max for UTF8
                    assertFalse(columnStatistics.isNumNullsSet());
                    assertEquals(columnStatistics.getNumNulls(), -1);
                    assertNull(columnStatistics.getMin());
                    assertNull(columnStatistics.getMax());
                    assertNull(columnStatistics.getMinBytes());
                    assertNull(columnStatistics.getMaxBytes());
                    assertNull(columnStatistics.genericGetMin());
                    assertNull(columnStatistics.genericGetMax());
                });

        // Corrupted stats written by Parquet before https://issues.apache.org/jira/browse/PARQUET-1025
        statistics = new Statistics();
        statistics.setNull_count(13);
        statistics.setMin("é".getBytes(UTF_8));
        statistics.setMax("a".getBytes(UTF_8));
        assertThat(MetadataReader.readStats(statistics, varchar))
                .isInstanceOfSatisfying(BinaryStatistics.class, columnStatistics -> {
                    // Stats ignored because we provided min/max for UTF8
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
        assertThat(MetadataReader.readStats(statistics, varchar))
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

    private static byte[] fromHex(String hex)
    {
        return BaseEncoding.base16().decode(hex);
    }
}
