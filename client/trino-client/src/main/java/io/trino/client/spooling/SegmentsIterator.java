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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class SegmentsIterator
        extends AbstractIterator<List<Object>>
        implements CloseableIterator<List<Object>>
{
    private final SegmentLoader loader;
    private final QueryDataDecoder decoder;
    private final Deque<Segment> remainingSegments;
    private CloseableIterator<List<Object>> currentIterator;

    public SegmentsIterator(SegmentLoader loader, QueryDataDecoder decoder, List<Segment> segments)
    {
        verify(!segments.isEmpty(), "Expected at least a single segment to iterate over");
        this.loader = requireNonNull(loader, "loader is null");
        this.decoder = requireNonNull(decoder, "decoder is null");
        this.remainingSegments = new ArrayDeque<>(segments);
        this.currentIterator = iterate(this.remainingSegments.removeFirst());
    }

    @Override
    protected List<Object> computeNext()
    {
        if (currentIterator.hasNext()) {
            return currentIterator.next();
        }

        // Current iterator exhausted, move to next one
        if (moveToNextIterator()) {
            verify(currentIterator.hasNext(), "New current iterator is empty");
            return currentIterator.next();
        }

        return endOfData();
    }

    private boolean moveToNextIterator()
    {
        if (!remainingSegments.isEmpty()) {
            Segment segment = remainingSegments.removeFirst();
            currentIterator = iterate(segment);
            return true;
        }
        else {
            return false;
        }
    }

    private CloseableIterator<List<Object>> iterate(Segment segment)
    {
        if (segment instanceof InlineSegment) {
            return new InlineSegmentIterator((InlineSegment) segment, decoder);
        }

        if (segment instanceof SpooledSegment) {
            return new SpooledSegmentIterator((SpooledSegment) segment, loader, decoder);
        }

        throw new UnsupportedOperationException("Unsupported segment type: " + segment.getClass().getName());
    }

    @Override
    public void close()
            throws IOException
    {
        IOException exception = new IOException("Could not close all segments");
        if (currentIterator != null) {
            try {
                currentIterator.close();
            }
            catch (IOException e) {
                exception.addSuppressed(e);
            }
        }

        for (Segment segment : remainingSegments) {
            try {
                iterate(segment).close();
            }
            catch (IOException e) {
                exception.addSuppressed(e);
            }
        }

        if (exception.getSuppressed().length > 0) {
            throw exception;
        }
    }

    @Override
    public String toString()
    {
        return "SegmentsIterator{currentIterator=" + currentIterator + ", remainingSegments=" + remainingSegments + "}";
    }
}
