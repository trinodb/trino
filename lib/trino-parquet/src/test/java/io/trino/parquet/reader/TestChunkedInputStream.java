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

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestChunkedInputStream
{
    @Test
    public void empty()
            throws IOException
    {
        assertThatThrownBy(() -> input(ImmutableList.of())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test(dataProvider = "chunks")
    public void testInput(List<byte[]> chunks)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] chunk : chunks) {
            out.write(chunk);
        }
        byte[] expectedBytes = out.toByteArray();
        byte[] buffer = new byte[expectedBytes.length + 1];

        List<Slice> slices = chunks.stream().map(Slices::wrappedBuffer).collect(toImmutableList());
        assertThat(input(slices).readAllBytes()).isEqualTo(expectedBytes);
        assertThat(input(slices).getSlice(expectedBytes.length).getBytes()).isEqualTo(expectedBytes);
        assertThat(readAll(input(slices))).isEqualTo(expectedBytes);

        assertThat(input(slices).readNBytes(expectedBytes.length)).isEqualTo(expectedBytes);

        assertThat(input(slices).read(buffer, 0, 0)).isEqualTo(0);
        assertThat(input(slices).getSlice(0)).isEqualTo(EMPTY_SLICE);

        if (expectedBytes.length > 0) {
            // read from one chunk only
            assertThat(input(slices).read(buffer, 0, 1)).isEqualTo(1);
            assertThat(buffer[0]).isEqualTo(expectedBytes[0]);
        }

        // rad more than total length
        ChunkedInputStream input = input(slices);
        int bytesRead = ByteStreams.read(input, buffer, 0, buffer.length);
        assertThat(bytesRead).isEqualTo(expectedBytes.length > 0 ? expectedBytes.length : -1);

        // read after input is done returns -1
        assertThat(input.read()).isEqualTo(-1);
        // getSlice(0) after input is done returns empty slice
        assertThat(input.getSlice(0)).isEqualTo(EMPTY_SLICE);
        assertThatThrownBy(() -> input.getSlice(1)).isInstanceOf(IllegalArgumentException.class);

        assertThat(input.read(buffer, 0, 1)).isEqualTo(-1);

        // verify available
        ChunkedInputStream availableInput = input(slices);
        // nothing is read initially
        assertThat(availableInput.available()).isEqualTo(0);
        for (byte[] chunk : chunks) {
            availableInput.read();
            assertThat(availableInput.available()).isEqualTo(chunk.length - 1);
            availableInput.skipNBytes(chunk.length - 1);
            assertThat(availableInput.available()).isEqualTo(0);
        }
    }

    @Test(dataProvider = "chunks")
    public void testClose(List<byte[]> chunks)
            throws IOException
    {
        List<Slice> slices = chunks.stream().map(Slices::wrappedBuffer).collect(toImmutableList());

        // close fresh input, not read
        List<TestingChunkReader> chunksReaders = slices.stream().map(TestingChunkReader::new).collect(toImmutableList());
        ChunkedInputStream input = new ChunkedInputStream(chunksReaders);
        input.close();
        for (TestingChunkReader chunksReader : chunksReaders) {
            assertThat(chunksReader.isFreed()).isTrue();
        }

        // close partially read input
        chunksReaders = slices.stream().map(TestingChunkReader::new).collect(toImmutableList());
        input = new ChunkedInputStream(chunksReaders);
        input.readNBytes(chunks.get(0).length);
        input.close();
        for (TestingChunkReader chunksReader : chunksReaders) {
            assertThat(chunksReader.isFreed()).isTrue();
        }

        // close fully read input
        chunksReaders = slices.stream().map(TestingChunkReader::new).collect(toImmutableList());
        input = new ChunkedInputStream(chunksReaders);
        input.readNBytes(chunks.stream().mapToInt(chunk -> chunk.length).sum());
        input.close();
        for (TestingChunkReader chunksReader : chunksReaders) {
            assertThat(chunksReader.isFreed()).isTrue();
        }
    }

    @DataProvider
    public Object[][] chunks()
    {
        return new Object[][] {
                {ImmutableList.of(new byte[] {1, 2, 3})},
                {ImmutableList.of(new byte[] {1, 2, 3}, new byte[] {1, 2})},
                {ImmutableList.of(new byte[] {1, 2, 3}, new byte[] {1}, new byte[] {1, 2})},
        };
    }

    private static byte[] readAll(ChunkedInputStream input)
            throws IOException
    {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int read;
        while ((read = input.read()) != -1) {
            out.write(read);
        }
        return out.toByteArray();
    }

    private static ChunkedInputStream input(List<Slice> slices)
    {
        return new ChunkedInputStream(slices.stream().map(TestingChunkReader::new).collect(toImmutableList()));
    }
}
