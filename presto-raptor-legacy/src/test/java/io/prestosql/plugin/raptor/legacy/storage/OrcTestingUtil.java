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
package io.prestosql.plugin.raptor.legacy.storage;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.UnsignedBytes;
import io.airlift.units.DataSize;
import io.prestosql.orc.FileOrcDataSource;
import io.prestosql.orc.OrcCorruptionException;
import io.prestosql.orc.OrcDataSource;
import io.prestosql.orc.OrcPredicate;
import io.prestosql.orc.OrcReader;
import io.prestosql.orc.OrcReaderOptions;
import io.prestosql.orc.OrcRecordReader;
import io.prestosql.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.MAX_BATCH_SIZE;
import static org.testng.Assert.assertEquals;

final class OrcTestingUtil
{
    private OrcTestingUtil() {}

    public static final OrcReaderOptions READER_OPTIONS = new OrcReaderOptions()
            .withMaxReadBlockSize(new DataSize(1, MEGABYTE))
            .withMaxMergeDistance(new DataSize(1, MEGABYTE))
            .withMaxBufferSize(new DataSize(1, MEGABYTE))
            .withStreamBufferSize(new DataSize(1, MEGABYTE))
            .withTinyStripeThreshold(new DataSize(1, MEGABYTE));

    public static OrcDataSource fileOrcDataSource(File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, READER_OPTIONS);
    }

    public static OrcRecordReader createReader(OrcDataSource dataSource, List<Long> columnIds, List<Type> types)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, READER_OPTIONS);

        List<String> columnNames = orcReader.getColumnNames();
        assertEquals(columnNames.size(), columnIds.size());

        Map<Integer, Type> includedColumns = new HashMap<>();
        int ordinal = 0;
        for (long columnId : columnIds) {
            assertEquals(columnNames.get(ordinal), String.valueOf(columnId));
            includedColumns.put(ordinal, types.get(ordinal));
            ordinal++;
        }

        return createRecordReader(orcReader, includedColumns);
    }

    public static OrcRecordReader createReaderNoRows(OrcDataSource dataSource)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(dataSource, READER_OPTIONS);

        assertEquals(orcReader.getColumnNames().size(), 0);

        return createRecordReader(orcReader, ImmutableMap.of());
    }

    public static OrcRecordReader createRecordReader(OrcReader orcReader, Map<Integer, Type> includedColumns)
            throws OrcCorruptionException
    {
        return orcReader.createRecordReader(includedColumns, OrcPredicate.TRUE, DateTimeZone.UTC, newSimpleAggregatedMemoryContext(), MAX_BATCH_SIZE);
    }

    public static byte[] octets(int... values)
    {
        byte[] bytes = new byte[values.length];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = UnsignedBytes.checkedCast(values[i]);
        }
        return bytes;
    }
}
