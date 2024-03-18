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

import io.airlift.log.Logger;
import io.trino.plugin.varada.storage.engine.StorageEngine;
import io.trino.plugin.varada.storage.engine.StorageEngineConstants;
import io.trino.plugin.varada.storage.juffers.WriteJuffersWarmUpElement;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexOutput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

public class VaradaIndexOutput
        extends IndexOutput
{
    private static final Logger logger = Logger.get(VaradaIndexOutput.class);
    private final StorageEngine storageEngine;
    private final boolean forwardToNative;
    private final LuceneFileType luceneFileType;
    private final StorageEngineConstants storageEngineConstants;
    private final WriteJuffersWarmUpElement juffersWE;
    private final long weCookie;
    private final ByteBuffersIndexOutput byteBuffersIndexOutput;

    public VaradaIndexOutput(String fileName,
            WriteJuffersWarmUpElement juffersWE,
            StorageEngineConstants storageEngineConstants,
            StorageEngine storageEngine,
            long weCookie,
            boolean forwardToNative)
    {
        super("VaradaLuceneIndex", "varadaLucene");
        this.storageEngine = storageEngine;
        this.forwardToNative = forwardToNative;
        this.luceneFileType = LuceneFileType.getType(fileName);
        this.storageEngineConstants = storageEngineConstants;
        this.weCookie = weCookie;
        this.juffersWE = juffersWE;
        this.byteBuffersIndexOutput = new ByteBuffersIndexOutput(new ByteBuffersDataOutput(), "VaradaLuceneIndex", "varadaLucene");
    }

    @Override
    public void copyBytes(DataInput input, long numBytes)
            throws IOException
    {
        if (numBytes > Integer.MAX_VALUE) {
            throw new RuntimeException("too large copy of " + numBytes + " bytes");
        }

        if (forwardToNative && (luceneFileType != LuceneFileType.UNKNOWN)) {
            logger.debug("weCookie %x, LUCENE INDEX %s before copyBytes called %d", weCookie, super.getName(), numBytes);
            int bufferSize = luceneFileType.isSmallFile() ? storageEngineConstants.getLuceneSmallJufferSize() : storageEngineConstants.getLuceneBigJufferSize();
            writeToJuffer(bufferSize, input, (int) numBytes); // we enter this if only if number of bytes is lower than max integer
            logger.debug("weCookie %x, LUCENE INDEX %s after copyBytes called %d", weCookie, super.getName(), numBytes);
            return;
        }

        byteBuffersIndexOutput.copyBytes(input, numBytes);
    }

    private void writeToJuffer(int bufferSize, DataInput input, int numBytes)
    {
        ByteBuffer luceneFileBuffer = juffersWE.getLuceneFileBuffer(luceneFileType);
        int bytesLeftToRead = numBytes;
        int fileOffset = 0;
        byte[] readBytes = new byte[bufferSize];

        while (bytesLeftToRead > 0) {
            int bytesToRead = Math.min(bufferSize, bytesLeftToRead);
            try {
                luceneFileBuffer.position(0);
                input.readBytes(readBytes, 0, bytesToRead);
                luceneFileBuffer.put(readBytes, 0, bytesToRead);
                logger.debug("weCookie %x, before write buffer file %s(%d), offset %d, length %d", weCookie, luceneFileType, luceneFileType.getNativeId(), fileOffset, bytesToRead);
                storageEngine.luceneWriteBuffer(weCookie, luceneFileType.getNativeId(), fileOffset, bytesToRead);
            }
            catch (IOException e) {
                logger.warn(e, "Failed to write buffer. weCookie %x", weCookie);
                throw new RuntimeException(e);
            }
            bytesLeftToRead -= bytesToRead;
            fileOffset += bytesToRead;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        byteBuffersIndexOutput.close();
    }

    @Override
    public long getFilePointer()
    {
        return byteBuffersIndexOutput.getFilePointer();
    }

    @Override
    public long getChecksum()
            throws IOException
    {
        return byteBuffersIndexOutput.getChecksum();
    }

    @Override
    public void writeByte(byte b)
            throws IOException
    {
        byteBuffersIndexOutput.writeByte(b);
    }

    @Override
    public void writeBytes(byte[] b, int offset, int length)
            throws IOException
    {
        byteBuffersIndexOutput.writeBytes(b, offset, length);
    }

    @Override
    public void writeBytes(byte[] b, int length)
            throws IOException
    {
        byteBuffersIndexOutput.writeBytes(b, length);
    }

    @Override
    public void writeInt(int i)
            throws IOException
    {
        byteBuffersIndexOutput.writeInt(i);
    }

    @Override
    public void writeShort(short i)
            throws IOException
    {
        byteBuffersIndexOutput.writeShort(i);
    }

    @Override
    public void writeLong(long i)
            throws IOException
    {
        byteBuffersIndexOutput.writeLong(i);
    }

    @Override
    public void writeString(String s)
            throws IOException
    {
        byteBuffersIndexOutput.writeString(s);
    }

    @Override
    public void writeMapOfStrings(Map<String, String> map)
            throws IOException
    {
        byteBuffersIndexOutput.writeMapOfStrings(map);
    }

    @Override
    public void writeSetOfStrings(Set<String> set)
            throws IOException
    {
        byteBuffersIndexOutput.writeSetOfStrings(set);
    }
}
