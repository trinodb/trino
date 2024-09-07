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
package io.trino.filesystem.alluxio;

import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.conf.AlluxioConfiguration;
import io.opentelemetry.api.trace.Tracer;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;

import java.io.EOFException;
import java.io.IOException;

import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class AlluxioInput
        implements TrinoInput
{
    private final TrinoInputFile inputFile;
    private final long fileLength;
    private final AlluxioCacheStats statistics;
    private final AlluxioInputHelper helper;

    private TrinoInput input;
    private boolean closed;

    public AlluxioInput(
            Tracer tracer,
            TrinoInputFile inputFile,
            String cacheKey,
            URIStatus status,
            CacheManager cacheManager,
            AlluxioConfiguration configuration,
            AlluxioCacheStats statistics)
    {
        this.inputFile = requireNonNull(inputFile, "inputFile is null");
        this.fileLength = requireNonNull(status, "status is null").getLength();
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.helper = new AlluxioInputHelper(tracer, inputFile.location(), cacheKey, status, cacheManager, configuration, statistics);
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(offset, length, buffer.length);
        if (position < 0) {
            throw new IOException("Negative seek offset");
        }
        if (length == 0) {
            return;
        }

        int bytesRead = helper.doCacheRead(position, buffer, offset, length);
        if (length > bytesRead && position + bytesRead == fileLength) {
            throw new EOFException("Read %s of %s requested bytes: %s".formatted(bytesRead, length, inputFile.location()));
        }
        doExternalRead(position + bytesRead, buffer, offset + bytesRead, length - bytesRead);
    }

    private int doExternalRead(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        if (length == 0) {
            return 0;
        }

        AlluxioInputHelper.PageAlignedRead aligned = helper.alignRead(position, length);
        byte[] readBuffer = new byte[aligned.length()];
        getInput().readFully(aligned.pageStart(), readBuffer, 0, readBuffer.length);
        helper.putCache(aligned.pageStart(), aligned.pageEnd(), readBuffer, aligned.length());
        System.arraycopy(readBuffer, aligned.pageOffset(), buffer, offset, length);
        statistics.recordExternalRead(readBuffer.length);
        return length;
    }

    private TrinoInput getInput()
            throws IOException
    {
        if (input == null) {
            input = inputFile.newInput();
        }
        return input;
    }

    @Override
    public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        ensureOpen();
        checkFromIndexSize(bufferOffset, bufferLength, buffer.length);

        int readSize = (int) min(fileLength, bufferLength);
        readFully(fileLength - readSize, buffer, bufferOffset, readSize);
        return readSize;
    }

    private void ensureOpen()
            throws IOException
    {
        if (closed) {
            throw new IOException("Stream closed: " + inputFile.location());
        }
    }

    @Override
    public void close()
            throws IOException
    {
        closed = true;
        if (input != null) {
            input.close();
        }
    }
}
