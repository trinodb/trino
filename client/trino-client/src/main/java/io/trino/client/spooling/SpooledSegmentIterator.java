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
package io.trino.client.spooling;

import com.google.common.collect.AbstractIterator;
import com.google.common.io.Closer;
import io.trino.client.CloseableIterator;
import io.trino.client.QueryDataDecoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

// Accessible through the SpooledSegment.toIterator
class SpooledSegmentIterator
        extends AbstractIterator<List<Object>>
        implements CloseableIterator<List<Object>>
{
    private final SpooledSegment segment;
    private final long rowsCount;
    private final SegmentLoader loader;
    private final QueryDataDecoder decoder;
    private final Closer closer = Closer.create();
    private long currentRow;
    private boolean loaded;
    private boolean closed;
    private Iterator<List<Object>> iterator;

    public SpooledSegmentIterator(SpooledSegment spooledSegment, SegmentLoader loader, QueryDataDecoder decoder)
    {
        this.segment = requireNonNull(spooledSegment, "spooledSegment is null");
        this.rowsCount = spooledSegment.getRowsCount();
        this.loader = requireNonNull(loader, "loader is null");
        this.decoder = requireNonNull(decoder, "decoder is null");

        closer.register(() -> loader.acknowledge(segment)); // acknowledge segment when closed
    }

    public void load()
    {
        checkState(!closed, "Iterator is already closed");
        checkState(!loaded, "Iterator is already loaded");

        checkState(iterator == null, "Iterator should be unloaded");
        try {
            InputStream stream = closer.register(loader.load(segment)); // close stream when exhausted
            iterator = closer.register(decoder.decode(stream, segment.getMetadata()));
            loaded = true;
        }
        catch (IOException e) {
            closed = true;
            throw new UncheckedIOException(e);
        }
    }

    public void unload()
    {
        checkState(!closed, "Iterator is already closed");
        closed = true;
        try {
            closer.close();
            iterator = null;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected List<Object> computeNext()
    {
        if (!loaded) {
            load();
        }

        if (++currentRow > rowsCount) {
            return endOfData();
        }

        if (closed) {
            throw new NoSuchElementException();
        }

        try {
            verify(iterator.hasNext(), "Iterator should have more rows, current: %s, count: %s", currentRow, rowsCount);
            List<Object> rows = iterator.next();
            if (currentRow == this.rowsCount) {
                unload(); // Unload when the last row was fetched
            }
            return rows;
        }
        catch (Exception e) {
            // Cleanup if decoding has failed
            unload();
            throw e;
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed) {
            unload();
        }
    }

    @Override
    public String toString()
    {
        return "SpooledSegmentIterator{segment=" + segment + "}";
    }
}
