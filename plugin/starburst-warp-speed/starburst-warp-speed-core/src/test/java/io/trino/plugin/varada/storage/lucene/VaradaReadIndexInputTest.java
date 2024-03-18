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
package io.trino.plugin.varada.storage.lucene;

import io.trino.plugin.varada.juffer.BufferAllocator;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import io.trino.plugin.varada.storage.read.StorageReaderTestUtils;
import io.trino.plugin.warp.gen.constants.JbufType;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import org.apache.lucene.store.IndexInput;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VaradaReadIndexInputTest
{
    private static final int SMALL_FILE_SIZE = 20;
    private VaradaReadIndexInput varadaReadIndexInput;
    private StorageEngine storageEngine;

    @BeforeEach
    public void before()
    {
        BufferAllocator bufferAllocator = mock(BufferAllocator.class);
        when(bufferAllocator.ids2NullBuff(any())).thenReturn(allocateByteBuffer());
        when(bufferAllocator.ids2RecBuff(any())).thenReturn(allocateByteBuffer());
        when(bufferAllocator.ids2LuceneBuffers(any())).thenReturn(allocateLuceneByteBuffers());
        when(bufferAllocator.ids2LuceneResultBM(any())).thenReturn(allocateByteBuffer());

        long[] outColBuffIds = new long[JbufType.JBUF_TYPE_NUM_OF.ordinal()];
        StorageReaderTestUtils readerTestUtils = new StorageReaderTestUtils();
        readerTestUtils.init();
        StorageEngineConstants storageEngineConstants = mock(StorageEngineConstants.class);
        ReadJuffersWarmUpElement dataRecordJuffer = new ReadJuffersWarmUpElement(bufferAllocator, false, true);
        dataRecordJuffer.createBuffers(RecTypeCode.REC_TYPE_VARCHAR, 10, false, outColBuffIds);
        storageEngine = readerTestUtils.getStorageEngine();
        when(storageEngineConstants.getLuceneSmallJufferSize()).thenReturn(SMALL_FILE_SIZE);
        varadaReadIndexInput = new VaradaReadIndexInput(storageEngine,
                storageEngineConstants,
                dataRecordJuffer,
                LuceneFileType.SEGMENTS,
                1,
                0,
                100,
                "root");
    }

    @Test
    public void testReadBytesLessThenPageSizeShouldFetchOnce()
    {
        for (int i = 0; i < SMALL_FILE_SIZE - 1; i++) {
            varadaReadIndexInput.readByte();
        }
        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(0), eq(SMALL_FILE_SIZE));
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(SMALL_FILE_SIZE - 1);
    }

    @Test
    public void testReadBytesMoreThenPageSizeShouldFetchWhenNeeded()
    {
        for (int i = 0; i < SMALL_FILE_SIZE; i++) {
            varadaReadIndexInput.readByte();
        }
        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(0), eq(SMALL_FILE_SIZE));

        varadaReadIndexInput.readByte();
        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(SMALL_FILE_SIZE), eq(SMALL_FILE_SIZE));
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(1);
    }

    @Test
    public void testReadBytes()
            throws IOException
    {
        int numOfBytesToRead = 5;
        varadaReadIndexInput.readInt();
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(Integer.BYTES);
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(Integer.BYTES);

        byte[] readBytes = new byte[numOfBytesToRead];
        varadaReadIndexInput.readBytes(readBytes, 0, numOfBytesToRead);
        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(0), eq(SMALL_FILE_SIZE));
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(Integer.BYTES + numOfBytesToRead);
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(Integer.BYTES + numOfBytesToRead);
    }

    @Test
    public void testReadIntMoreThenPageSizeShouldFetchWhenNeeded()
            throws IOException
    {
        for (int i = 0; i < SMALL_FILE_SIZE / Integer.BYTES; i++) {
            varadaReadIndexInput.readInt();
        }
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(SMALL_FILE_SIZE);

        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(0), eq(SMALL_FILE_SIZE));

        varadaReadIndexInput.readInt();
        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(SMALL_FILE_SIZE), eq(SMALL_FILE_SIZE));
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(4);
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(SMALL_FILE_SIZE + Integer.BYTES);
    }

    @Test
    public void testSeekShouldResetRecordBuffer()
            throws IOException
    {
        varadaReadIndexInput.seek(10);
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(10);
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(10);

        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), anyInt(), anyInt());
    }

    @Test
    public void testSkipBytes()
            throws IOException
    {
        int bytesToSkip = 10;
        varadaReadIndexInput.readInt();
        varadaReadIndexInput.skipBytes(bytesToSkip);
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo(bytesToSkip + Integer.BYTES);
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(bytesToSkip + Integer.BYTES);

        verify(storageEngine, times(1)).luceneReadBuffer(anyLong(), anyInt(), eq(0), eq(SMALL_FILE_SIZE));
    }

    @Test
    public void testSkipBytesShouldFetchAndAlign()
            throws IOException
    {
        int skipSize = SMALL_FILE_SIZE - 1;
        varadaReadIndexInput.readInt();
        varadaReadIndexInput.skipBytes(skipSize);
        assertThat(varadaReadIndexInput.getBufferPosition()).isEqualTo((skipSize + Integer.BYTES) % SMALL_FILE_SIZE);
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(skipSize + Integer.BYTES);

        verify(storageEngine, times(2)).luceneReadBuffer(anyLong(), anyInt(), anyInt(), anyInt());
    }

    @Test
    public void testSlice()
            throws IOException
    {
        varadaReadIndexInput.readInt();
        long beforeSliceFilePointer = varadaReadIndexInput.getFilePointer();
        IndexInput slice = varadaReadIndexInput.slice("stam desc", 3, 10);
        assertThat(slice.getFilePointer()).isEqualTo(0); // new slice init it filePointer
        assertThat(slice.length()).isEqualTo(10);
        slice.seek(5);
        assertThat(slice.getFilePointer()).isEqualTo(5);
        assertThat(varadaReadIndexInput.getFilePointer()).isEqualTo(beforeSliceFilePointer); // slice operation should not impact original indexInput
    }

    @Test
    public void testSliceFromSlice()
            throws IOException
    {
        int firstOffset = 3;
        int secondOffset = 5;
        int firstSeek = 50;
        int secondSeek = 23;
        varadaReadIndexInput.readInt();

        VaradaReadIndexInput firstSlice = (VaradaReadIndexInput) varadaReadIndexInput.slice("stam desc", firstOffset, 80);
        assertThat(firstSlice.getFilePointer()).isEqualTo(0); // new firstSlice init it filePointer
        assertThat(firstSlice.length()).isEqualTo(80);
        firstSlice.seek(firstSeek);
        assertThat(firstSlice.getAbsolutePostion()).isEqualTo(firstSeek + firstOffset);
        assertThat(firstSlice.getFilePointer()).isEqualTo(firstSeek);
        assertThat(firstSlice.getBufferPosition()).isEqualTo(firstSeek % SMALL_FILE_SIZE + firstOffset);

        VaradaReadIndexInput secondSlice = (VaradaReadIndexInput) firstSlice.slice("second slice", secondOffset, 50);
        firstSlice.readByte();
        assertThat(firstSlice.getAbsolutePostion()).isEqualTo(firstSeek + firstOffset + 1);
        assertThat(firstSlice.getBufferPosition()).isEqualTo((firstSeek + firstOffset) % SMALL_FILE_SIZE + 1);
        assertThat(firstSlice.getFilePointer()).isEqualTo(firstSeek + 1);

        assertThat(secondSlice.length()).isEqualTo(50);
        secondSlice.readByte();
        assertThat(secondSlice.getFilePointer()).isEqualTo(1);
        secondSlice.seek(secondSeek);
        assertThat(secondSlice.getFilePointer()).isEqualTo(secondSeek);
        assertThat(secondSlice.getBufferPosition()).isEqualTo((secondSeek + firstOffset + secondOffset) % SMALL_FILE_SIZE);

        assertThat(secondSlice.getAbsolutePostion()).isEqualTo(secondSeek + firstOffset + secondOffset);

        // check that origin slice wasn't changed
        assertThat(firstSlice.getAbsolutePostion()).isEqualTo(firstSeek + firstOffset + 1);
        assertThat(firstSlice.getBufferPosition()).isEqualTo((firstSeek + firstOffset) % SMALL_FILE_SIZE + 1);
        assertThat(firstSlice.getFilePointer()).isEqualTo(firstSeek + 1);
    }

    @Test
    public void testClone()
            throws IOException
    {
        varadaReadIndexInput.readInt();
        varadaReadIndexInput.readInt();

        VaradaReadIndexInput clone = (VaradaReadIndexInput) varadaReadIndexInput.clone();
        assertThat(clone.getFilePointer()).isEqualTo(varadaReadIndexInput.getFilePointer());
        assertThat(clone.getBufferPosition()).isEqualTo(varadaReadIndexInput.getBufferPosition());
        clone.readInt();
        assertThat(clone.getFilePointer()).isNotEqualTo(varadaReadIndexInput.getFilePointer());
        assertThat(clone.getBufferPosition()).isNotEqualTo(varadaReadIndexInput.getBufferPosition());
    }

    ByteBuffer allocateByteBuffer()
    {
        return ByteBuffer.allocate(100);
    }

    ByteBuffer[] allocateLuceneByteBuffers()
    {
        return IntStream.range(0, 4).mapToObj((i) -> allocateByteBuffer()).toList().toArray(new ByteBuffer[0]);
    }
}
