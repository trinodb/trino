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
package io.trino.hive.formats.line;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FooterAwareLineReader
        implements LineReader
{
    private final LineReader delegate;
    // the value of the next footerCount rows
    private final Queue<LineBuffer> buffer;
    // the buffer that will be used to read the next row
    private LineBuffer nextBuffer;

    public FooterAwareLineReader(LineReader delegate, int footerCount, Supplier<LineBuffer> lineBufferSupplier)
            throws IOException
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.buffer = new ArrayDeque<>(footerCount);

        checkArgument(footerCount > 0, "footerCount must be at least 1");
        nextBuffer = lineBufferSupplier.get();

        // load the first footerCount rows into the buffer
        for (int i = 0; i < footerCount; i++) {
            bufferValue(lineBufferSupplier.get());
        }
    }

    @Override
    public boolean isClosed()
    {
        return delegate.isClosed();
    }

    @Override
    public long getRetainedSize()
    {
        return delegate.getRetainedSize();
    }

    @Override
    public long getBytesRead()
    {
        return delegate.getBytesRead();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean readLine(LineBuffer lineBuffer)
            throws IOException
    {
        lineBuffer.reset();

        if (nextBuffer != null) {
            bufferValue(nextBuffer);
        }

        // the next buffered row uses the last returned value
        nextBuffer = buffer.poll();
        if (nextBuffer != null) {
            copyValue(nextBuffer, lineBuffer);
        }
        return nextBuffer != null;
    }

    @Override
    public void close()
            throws IOException
    {
        nextBuffer = null;
        buffer.clear();
        delegate.close();
    }

    private void bufferValue(LineBuffer lineBuffer)
            throws IOException
    {
        if (delegate.readLine(lineBuffer)) {
            buffer.add(lineBuffer);
        }
        else {
            // there is not a next value, so the values in the buffer are from the footer and are ignored
            close();
        }
    }

    private static void copyValue(LineBuffer source, LineBuffer destination)
            throws IOException
    {
        destination.reset();
        destination.write(source.getBuffer(), 0, source.getLength());
    }
}
