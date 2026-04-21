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
import io.trino.client.CloseableIterator;
import io.trino.client.QueryDataDecoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

// Accessible through the InlineSegment.toIterator
class InlineSegmentIterator
        extends AbstractIterator<List<Object>>
        implements CloseableIterator<List<Object>>
{
    private InlineSegment segment;
    private final QueryDataDecoder decoder;
    private final long rowsCount;
    private CloseableIterator<List<Object>> iterator;
    private long currentRow;

    public InlineSegmentIterator(InlineSegment segment, QueryDataDecoder decoder)
    {
        this.segment = requireNonNull(segment, "segment is null");
        this.decoder = requireNonNull(decoder, "decoder is null");
        this.rowsCount = segment.getRowsCount();
    }

    @Override
    protected List<Object> computeNext()
    {
        if (iterator == null) {
            try {
                iterator = decoder.decode(new ByteArrayInputStream(segment.getData()), segment.getMetadata());
                segment = null;
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        if (currentRow == rowsCount) {
            unload();
            return endOfData();
        }

        currentRow++;
        verify(iterator.hasNext(), "Iterator should have more rows, current: %s, count: %s", currentRow, rowsCount);
        return iterator.next();
    }

    private void unload()
    {
        try {
            close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (iterator != null) {
            iterator.close();
            iterator = null;
        }
    }
}
