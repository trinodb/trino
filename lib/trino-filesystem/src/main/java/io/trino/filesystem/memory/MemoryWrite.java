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
package io.trino.filesystem.memory;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.Write;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

public class MemoryWrite
        implements Write
{
    private enum State
    {
        READY,
        ABORTED,
        FINISHED,
        CLOSED,
        /**/;
    }

    public interface OnStreamClose
    {
        void onCommit(Slice data)
                throws IOException;
    }

    private final Object lock = new Object();
    private final OnStreamClose onCommit;
    @GuardedBy("lock")
    private ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    @GuardedBy("lock")
    private State state = State.READY;
    private final MemoryOutputStream outputStream;

    public MemoryWrite(Location location, OnStreamClose onCommit)
    {
        this.onCommit = requireNonNull(onCommit, "onCommit is null");
        this.outputStream = new MemoryOutputStream(location);
    }

    @Override
    public OutputStream stream()
    {
        return outputStream;
    }

    @Override
    public void finish()
    {
        synchronized (lock) {
            checkState(state == State.READY, "Cannot finish in state %s", state);
            state = State.FINISHED;
        }
    }

    @Override
    public void abort()
    {
        synchronized (lock) {
            checkState(state == State.READY, "Cannot abort in state %s", state);
            state = State.ABORTED;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        synchronized (lock) {
            switch (state) {
                case READY, ABORTED -> {
                    boolean wasAborted = state == State.ABORTED;
                    state = State.CLOSED;
                    buffer = null;
                    if (!wasAborted) {
                        throw new IllegalStateException("Neither finish() nor abort() was called before close()");
                    }
                }

                case FINISHED -> {
                    state = State.CLOSED;
                    byte[] data = buffer.toByteArray();
                    buffer = null;
                    onCommit.onCommit(Slices.wrappedBuffer(data));
                }

                case CLOSED -> {
                    // Already closed
                }
            }
        }
    }

    private class MemoryOutputStream
            extends OutputStream
    {
        private final Location location;
        private boolean closed;

        public MemoryOutputStream(Location location)
        {
            this.location = requireNonNull(location, "location is null");
        }

        @Override
        public void write(int b)
                throws IOException
        {
            synchronized (lock) {
                ensureOpen();
                buffer.write(b);
            }
        }

        @Override
        public void write(byte[] bytes, int offset, int length)
                throws IOException
        {
            checkFromIndexSize(offset, length, bytes.length);

            synchronized (lock) {
                ensureOpen();
                buffer.write(bytes, offset, length);
            }
        }

        @Override
        public void flush()
                throws IOException
        {
            synchronized (lock) {
                ensureOpen();
            }
        }

        @GuardedBy("lock")
        private void ensureOpen()
                throws IOException
        {
            if (closed || state != State.READY) {
                throw new IOException("Output stream closed: " + location);
            }
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
