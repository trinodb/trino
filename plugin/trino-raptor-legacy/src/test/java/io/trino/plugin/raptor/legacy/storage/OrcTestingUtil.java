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
package io.trino.plugin.raptor.legacy.storage;

import com.google.common.primitives.UnsignedBytes;
import io.airlift.units.DataSize;
import io.trino.orc.FileOrcDataSource;
import io.trino.orc.OrcDataSource;
import io.trino.orc.OrcPredicate;
import io.trino.orc.OrcReader;
import io.trino.orc.OrcReaderOptions;
import io.trino.orc.OrcRecordReader;
import io.trino.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.orc.OrcReader.MAX_BATCH_SIZE;
import static org.assertj.core.api.Assertions.assertThat;

final class OrcTestingUtil
{
    private OrcTestingUtil() {}

    private static final OrcReaderOptions READER_OPTIONS = new OrcReaderOptions()
            .withMaxReadBlockSize(DataSize.of(1, MEGABYTE))
            .withMaxMergeDistance(DataSize.of(1, MEGABYTE))
            .withMaxBufferSize(DataSize.of(1, MEGABYTE))
            .withStreamBufferSize(DataSize.of(1, MEGABYTE))
            .withTinyStripeThreshold(DataSize.of(1, MEGABYTE));

    public static OrcDataSource fileOrcDataSource(File file)
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file, READER_OPTIONS);
    }

    public static OrcRecordReader createReader(OrcDataSource dataSource, List<Long> columnIds, List<Type> types)
            throws IOException
    {
        OrcReader orcReader = OrcReader.createOrcReader(dataSource, READER_OPTIONS)
                .orElseThrow(() -> new RuntimeException("File is empty"));

        List<String> columnNames = orcReader.getColumnNames();
        assertThat(columnNames.size()).isEqualTo(columnIds.size());

        return orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                types,
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                MAX_BATCH_SIZE,
                RuntimeException::new);
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
