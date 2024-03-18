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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.ReadJuffersWarmUpElement;
import org.apache.lucene.store.BufferedChecksum;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;
import java.util.zip.Checksum;

import static java.lang.Math.toIntExact;

public class VaradaReadIndexInput
        extends ChecksumIndexInput
        implements Cloneable
{
    private static final Logger logger = Logger.get(VaradaReadIndexInput.class);

    private final StorageEngine storageEngine;
    private final StorageEngineConstants storageEngineConstants;
    private final ReadJuffersWarmUpElement dataRecordJuffer;
    private final LuceneFileType luceneFileType;
    private final long nativeCookie;
    private final int pageSize;
    private final Checksum digest;
    private final String logPrefix;
    private final long length;
    private final String sliceDescription;
    private final long sliceOffset;
    // concurrent slices of the same file modifying the position of the juffer so we must have local copy
    private final ByteBuffer localCopyBuffer;
    private final ByteBuffer nativeJuffer;
    private int currentPageIndex;
    private int bufferLength;

    VaradaReadIndexInput(StorageEngine storageEngine,
            StorageEngineConstants storageEngineConstants,
            ReadJuffersWarmUpElement juffersWE,
            LuceneFileType luceneFileType,
            long nativeCookie,
            long sliceOffset,
            long length,
            String sliceDescription)
    {
        super("VaradaReadIndexInput_" + nativeCookie + "_" + luceneFileType);
        this.storageEngine = storageEngine;
        this.storageEngineConstants = storageEngineConstants;
        this.dataRecordJuffer = juffersWE;
        this.luceneFileType = luceneFileType;
        this.nativeCookie = nativeCookie;
        this.length = length;
        this.sliceOffset = sliceOffset;
        this.sliceDescription = sliceDescription;
        this.digest = new BufferedChecksum(new CRC32());
        this.logPrefix = String.format("%d(%s_%s_%d-%d)", nativeCookie, luceneFileType, sliceDescription, sliceOffset, sliceOffset + length);

        this.pageSize = luceneFileType.isSmallFile() ? storageEngineConstants.getLuceneSmallJufferSize() : storageEngineConstants.getPageSize();
        this.nativeJuffer = juffersWE.getLuceneFileBuffer(luceneFileType);
        int size = Math.min(pageSize, toIntExact((sliceOffset % pageSize) + length));
        this.localCopyBuffer = ByteBuffer.allocate(size);
        currentPageIndex = (int) (sliceOffset / pageSize);
        setCurrentBuffer();
        localCopyBuffer.position((int) sliceOffset % pageSize);
    }

    @Override
    public long getChecksum()
    {
        return digest.getValue();
    }

    @Override
    public void close()
    {
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public byte readByte()
    {
        if (localCopyBuffer.position() == bufferLength) {
            nextPage();
        }
        byte b = localCopyBuffer.get();
        digest.update(b);
        return b;
    }

    @Override
    public void readBytes(byte[] dst, int offset, int len)
    {
        int iterationLen = len;
        int iterationOffset = offset;
        while (iterationLen > 0) {
            if (localCopyBuffer.position() == bufferLength) {
                nextPage();
            }

            int remainInBuffer = bufferLength - localCopyBuffer.position();
            int bytesToCopy = Math.min(iterationLen, remainInBuffer);
            byte[] localSrc = new byte[bytesToCopy];
            localCopyBuffer.get(localSrc, 0, bytesToCopy);
            System.arraycopy(localSrc, 0, dst, iterationOffset, bytesToCopy);
            iterationOffset += bytesToCopy;
            iterationLen -= bytesToCopy;
        }
        digest.update(dst, offset, len);
    }

    private void nextPage()
    {
        currentPageIndex++;
        setCurrentBuffer();
    }

    private void loadPage()
    {
        // read upto one page
        int pageAlignOffset = currentPageIndex * pageSize;
        int fetchedBytes = (int) Math.min(pageSize, (sliceOffset + length - pageAlignOffset));
        storageEngine.luceneReadBuffer(nativeCookie, luceneFileType.getNativeId(), pageAlignOffset, fetchedBytes);

        // copy the content
        localCopyBuffer.position(0);
        nativeJuffer.position(0);
        int oldLimit = nativeJuffer.limit();
        nativeJuffer.limit(fetchedBytes);
        localCopyBuffer.put(nativeJuffer); // copy fetchedBytes bytes from buff to buff
        nativeJuffer.limit(oldLimit);
        nativeJuffer.position(0);
        localCopyBuffer.position(0);
    }

    /**
     * @return the position in the slice
     */
    @Override
    public long getFilePointer()
    {
        return ((long) currentPageIndex * pageSize + localCopyBuffer.position()) - sliceOffset;
    }

    long getAbsolutePostion()
    {
        return getFilePointer() + sliceOffset;
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        int newBufferIndex = (int) ((pos + sliceOffset) / pageSize);
        if (newBufferIndex != currentPageIndex) {
            // we seek'd to a different buffer:
            currentPageIndex = newBufferIndex;
            setCurrentBuffer();
        }

        localCopyBuffer.position((int) (pos + sliceOffset) % pageSize);
        // This is not >= because seeking to exact end of file is OK: this is where
        // you'd also be if you did a readBytes of all bytes in the file
        if (getFilePointer() > length()) {
            throw new EOFException("seek beyond EOF: pos=" + getFilePointer() + " vs length=" + length() + ": " + this);
        }
    }

    private void setCurrentBuffer()
    {
        loadPage();
        bufferLength = (int) Math.min(pageSize, (sliceOffset + length - ((long) currentPageIndex * pageSize)));
    }

    @Override
    public IndexInput slice(String sliceDescription, final long offset, final long sliceLength)
    {
        return new VaradaReadIndexInput(VaradaReadIndexInput.this.storageEngine, VaradaReadIndexInput.this.storageEngineConstants, VaradaReadIndexInput.this.dataRecordJuffer,
                VaradaReadIndexInput.this.luceneFileType, VaradaReadIndexInput.this.nativeCookie, offset + sliceOffset,
                sliceLength, sliceDescription);
    }

    @Override
    public IndexInput clone()
    {
        IndexInput ret = new VaradaReadIndexInput(storageEngine, storageEngineConstants, dataRecordJuffer, luceneFileType,
                nativeCookie, sliceOffset, length, sliceDescription);
        try {
            ret.seek(getFilePointer());
        }
        catch (IOException e) {
            logger.error(e, "txId=%s, failed clone", logPrefix);
            throw new RuntimeException(e);
        }
        return ret;
    }

    @VisibleForTesting
    int getBufferPosition()
    {
        return localCopyBuffer.position();
    }
}
