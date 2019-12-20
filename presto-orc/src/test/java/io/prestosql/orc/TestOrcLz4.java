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
package io.prestosql.orc;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.prestosql.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.prestosql.orc.OrcReader.INITIAL_BATCH_SIZE;
import static io.prestosql.orc.metadata.CompressionKind.LZ4;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

public class TestOrcLz4
{
    private static final DataSize SIZE = new DataSize(1, MEGABYTE);

    @Test
    public void testReadLz4()
            throws Exception
    {
        // this file was written with Apache ORC
        // TODO: use Apache ORC library in OrcTester
        byte[] data = toByteArray(getResource("apache-lz4.orc"));

        OrcReader orcReader = new OrcReader(new InMemoryOrcDataSource(data), new OrcReaderOptions());

        assertEquals(orcReader.getCompressionKind(), LZ4);
        assertEquals(orcReader.getFooter().getNumberOfRows(), 10_000);

        OrcRecordReader reader = orcReader.createRecordReader(
                orcReader.getRootColumn().getNestedColumns(),
                ImmutableList.of(BIGINT, INTEGER, BIGINT),
                OrcPredicate.TRUE,
                DateTimeZone.UTC,
                newSimpleAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE,
                RuntimeException::new);

        int rows = 0;
        while (true) {
            Page page = reader.nextPage();
            if (page == null) {
                break;
            }
            page = page.getLoadedPage();
            rows += page.getPositionCount();

            Block xBlock = page.getBlock(0);
            Block yBlock = page.getBlock(1);
            Block zBlock = page.getBlock(2);

            for (int position = 0; position < page.getPositionCount(); position++) {
                BIGINT.getLong(xBlock, position);
                INTEGER.getLong(yBlock, position);
                BIGINT.getLong(zBlock, position);
            }
        }

        assertEquals(rows, reader.getFileRowCount());
    }

    private static class InMemoryOrcDataSource
            extends AbstractOrcDataSource
    {
        private final byte[] data;

        public InMemoryOrcDataSource(byte[] data)
        {
            super(new OrcDataSourceId("memory"), data.length, new OrcReaderOptions());
            this.data = data;
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            System.arraycopy(data, toIntExact(position), buffer, bufferOffset, bufferLength);
        }
    }
}
