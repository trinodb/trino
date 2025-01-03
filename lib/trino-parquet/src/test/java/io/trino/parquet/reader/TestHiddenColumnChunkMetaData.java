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

import com.google.common.collect.ImmutableSet;
import io.trino.parquet.crypto.HiddenColumnChunkMetaData;
import io.trino.parquet.crypto.HiddenColumnException;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Set;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiddenColumnChunkMetaData
{
    @Test
    public void testIsHiddenColumn()
    {
        ColumnChunkMetadata column = new HiddenColumnChunkMetaData(ColumnPath.fromDotString("a.b.c"),
                "hdfs:/foo/bar/a.parquet");
        assertThat(HiddenColumnChunkMetaData.isHiddenColumn(column)).isTrue();
    }

    @Test
    public void testIsNotHiddenColumn()
    {
        Set<Encoding> encodingSet = Collections.singleton(Encoding.RLE);
        EncodingStats encodingStats = new EncodingStats.Builder()
                .withV2Pages()
                .addDictEncoding(PLAIN)
                .addDataEncodings(ImmutableSet.copyOf(encodingSet)).build();
        PrimitiveType type = Types.optional(BINARY).named("");
        Statistics<?> stats = Statistics.createStats(type);
        ColumnChunkMetadata column = ColumnChunkMetadata.get(ColumnPath.fromDotString("a.b.c"), type,
                CompressionCodecName.GZIP, encodingStats, encodingSet, stats, -1, -1, -1, -1, -1);
        assertThat(HiddenColumnChunkMetaData.isHiddenColumn(column)).isFalse();
    }

    @Test(expectedExceptions = HiddenColumnException.class)
    public void testHiddenColumnException()
    {
        ColumnChunkMetadata column = new HiddenColumnChunkMetaData(ColumnPath.fromDotString("a.b.c"),
                "hdfs:/foo/bar/a.parquet");
        column.getStatistics();
    }

    @Test
    public void testNoHiddenColumnException()
    {
        Set<Encoding> encodingSet = Collections.singleton(Encoding.RLE);
        EncodingStats encodingStats = new EncodingStats.Builder()
                .withV2Pages()
                .addDictEncoding(PLAIN)
                .addDataEncodings(ImmutableSet.copyOf(encodingSet)).build();
        PrimitiveType type = Types.optional(BINARY).named("");
        Statistics<?> stats = Statistics.createStats(type);
        ColumnChunkMetadata column = ColumnChunkMetadata.get(ColumnPath.fromDotString("a.b.c"), type,
                CompressionCodecName.GZIP, encodingStats, encodingSet, stats, -1, -1, -1, -1, -1);
        column.getStatistics();
    }
}
