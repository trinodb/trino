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
package io.trino.plugin.hive.parquet;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ListMultimap;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.trino.parquet.ChunkReader;
import io.trino.parquet.DiskRange;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.hive.FileFormatDataSourceStats;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.PositionedReadable;
import org.apache.hadoop.fs.Seekable;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHdfsParquetDataSource
{
    @Test(dataProvider = "testPlanReadOrderingProvider")
    public void testPlanReadOrdering(DataSize maxBufferSize)
    {
        Slice testingInput = Slices.wrappedIntArray(IntStream.range(0, 1000).toArray());
        HdfsParquetDataSource dataSource = new HdfsParquetDataSource(
                new ParquetDataSourceId("test"),
                0,
                new FSDataInputStream(new TestingSliceInputStream(testingInput.getInput())),
                new FileFormatDataSourceStats(),
                new ParquetReaderOptions().withMaxBufferSize(maxBufferSize));

        ListMultimap<String, ChunkReader> chunkReaders = dataSource.planRead(ImmutableListMultimap.<String, DiskRange>builder()
                .putAll("test", new DiskRange(0, 300), new DiskRange(400, 100), new DiskRange(700, 200))
                .build());
        assertThat(chunkReaders.get("test"))
                .map(ChunkReader::read)
                .isEqualTo(ImmutableList.of(
                        testingInput.slice(0, 300),
                        testingInput.slice(400, 100),
                        testingInput.slice(700, 200)));
    }

    @DataProvider
    public Object[][] testPlanReadOrderingProvider()
    {
        return new Object[][] {
                {DataSize.ofBytes(0)}, // All large ranges
                {DataSize.ofBytes(200)}, // Mix of large and small ranges
                {DataSize.ofBytes(100000000)}, // All small ranges
        };
    }

    private static class TestingSliceInputStream
            extends InputStream
            implements Seekable, PositionedReadable
    {
        private final BasicSliceInput sliceInput;

        public TestingSliceInputStream(BasicSliceInput sliceInput)
        {
            this.sliceInput = requireNonNull(sliceInput, "sliceInput is null");
        }

        @Override
        public int read(long position, byte[] buffer, int offset, int length)
                throws IOException
        {
            long currentPosition = sliceInput.position();
            sliceInput.setPosition(position);
            int bytesRead = sliceInput.read(buffer, offset, length);
            sliceInput.setPosition(currentPosition);
            return bytesRead;
        }

        @Override
        public void readFully(long position, byte[] buffer, int offset, int length)
        {
            long currentPosition = sliceInput.position();
            sliceInput.setPosition(position);
            sliceInput.readFully(buffer, offset, length);
            sliceInput.setPosition(currentPosition);
        }

        @Override
        public void readFully(long position, byte[] buffer)
        {
            readFully(position, buffer, 0, buffer.length);
        }

        @Override
        public void seek(long pos)
        {
            sliceInput.setPosition(pos);
        }

        @Override
        public long getPos()
        {
            return sliceInput.position();
        }

        @Override
        public boolean seekToNewSource(long targetPos)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int read()
                throws IOException
        {
            return sliceInput.read();
        }
    }
}
