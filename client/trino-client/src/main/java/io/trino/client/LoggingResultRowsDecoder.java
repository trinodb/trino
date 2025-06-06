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
package io.trino.client;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.Segment;

import java.util.List;
import java.util.Optional;

import static io.trino.client.spooling.DataAttribute.ROW_OFFSET;
import static io.trino.client.spooling.DataAttribute.UNCOMPRESSED_SIZE;
import static java.lang.Math.round;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class LoggingResultRowsDecoder
        extends ResultRowsDecoder
{
    private final ResultRowsDecoder delegate;
    private long start = -1;

    public LoggingResultRowsDecoder(ResultRowsDecoder delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        System.err.println("Segment logging is enabled, segment details will be logged to stderr");
    }

    @Override
    public ResultRows toRows(QueryResults results)
    {
        return toRows(results.getColumns(), results.getData());
    }

    @Override
    public ResultRows toRows(List<Column> columns, QueryData data)
    {
        if (start == -1) {
            // Start the timer only once, when the first data frame is processed (could be empty)
            start = System.nanoTime();
        }
        Duration elapsed = new Duration(System.nanoTime() - start, NANOSECONDS).convertToMostSuccinctTimeUnit();
        if (data instanceof EncodedQueryData) {
            EncodedQueryData encodedData = (EncodedQueryData) data;

            for (Segment segment : encodedData.getSegments()) {
                long start = 1 + segment.getRequiredAttribute(ROW_OFFSET, Long.class);
                long rows = segment.getRowsCount();
                long end = start + rows - 1;
                boolean isInline = segment instanceof InlineSegment;

                int size = segment.getSegmentSize();
                long uncompressedSize = segment.getAttribute(UNCOMPRESSED_SIZE, Integer.class)
                        .orElse(size);

                boolean isCompressed = size != uncompressedSize;
                double compressionRatio = isCompressed ? round(10000.0d * size / uncompressedSize) / 100.0d : 1.0;

                System.err.printf("+ %9s %s rows %6d to %6d size: %9s uncompressed size: %9s compression ratio: %.02f%%%n",
                        elapsed,
                        isInline ? "Inlined" : "Spooled",
                        start,
                        end,
                        DataSize.ofBytes(size).succinct(),
                        DataSize.ofBytes(uncompressedSize).succinct(),
                        compressionRatio);
            }
        }

        if (data instanceof JsonQueryData) {
            JsonQueryData jsonData = (JsonQueryData) data;
            System.err.printf("+ %9s Direct rows %6d%n", elapsed, jsonData.getRowsCount());
        }

        if (data == null || data.isNull()) {
            System.err.printf("+ %9s Rows are not ready%n", elapsed);
        }

        return delegate.toRows(columns, data);
    }

    @Override
    public Optional<String> getEncoding()
    {
        return delegate.getEncoding();
    }

    @Override
    public void close()
            throws Exception
    {
        System.err.printf("+ %9s No more data %n", new Duration(System.nanoTime() - start, NANOSECONDS).convertToMostSuccinctTimeUnit());
        delegate.close();
    }
}
