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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.InlineSegment;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.SegmentLoader;
import io.trino.client.spooling.SpooledSegment;
import io.trino.client.spooling.encoding.QueryDataDecoders;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.client.FixJsonDataUtils.fixData;
import static io.trino.client.ResultRows.NULL_ROWS;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Class responsible for decoding any QueryData type.
 */
public class ResultRowsDecoder
        implements AutoCloseable
{
    private final SegmentLoader loader;
    private QueryDataDecoder.Factory decoderFactory;
    private List<Column> columns = emptyList();

    public ResultRowsDecoder()
    {
        this(new SegmentLoader());
    }

    public ResultRowsDecoder(SegmentLoader loader)
    {
        this.loader = requireNonNull(loader, "loader is null");
    }

    public ResultRowsDecoder withEncoding(String encoding)
    {
        if (decoderFactory != null) {
            if (!encoding.equals(decoderFactory.encoding())) {
                throw new IllegalStateException("Already set encoding " + encoding + " is not equal to " + decoderFactory.encoding());
            }
        }
        else {
            this.decoderFactory = QueryDataDecoders.get(encoding);
        }
        return this;
    }

    public ResultRowsDecoder withColumns(List<Column> columns)
    {
        if (this.columns.isEmpty()) {
            this.columns = ImmutableList.copyOf(columns);
        }
        else if (!columns.equals(this.columns)) {
            throw new IllegalStateException("Already set columns " + columns + " are not equal to " + this.columns);
        }

        return this;
    }

    public ResultRows toRows(QueryData data)
    {
        if (data == null) {
            return NULL_ROWS; // for backward compatibility instead of null
        }

        verify(!columns.isEmpty(), "columns must be set");
        if (data instanceof RawQueryData) {
            RawQueryData rawData = (RawQueryData) data;
            if (rawData.isNull()) {
                return NULL_ROWS; // for backward compatibility instead of null
            }
            return () -> fixData(columns, rawData.getIterable()).iterator();
        }

        if (data instanceof EncodedQueryData) {
            verify(decoderFactory != null, "decoderFactory must be set");
            // we don't need query-level attributes for now
            QueryDataDecoder decoder = decoderFactory.create(columns, DataAttributes.empty());
            EncodedQueryData encodedData = (EncodedQueryData) data;
            verify(decoder.encoding().equals(encodedData.getEncoding()), "encoding %s is not equal to %s", encodedData.getEncoding(), decoder.encoding());

            List<ResultRows> resultRows = encodedData.getSegments()
                    .stream()
                    .map(segment -> segmentToRows(decoder, segment))
                    .collect(toImmutableList());

            return concat(resultRows);
        }

        throw new UnsupportedOperationException("Unsupported data type: " + data.getClass().getName());
    }

    private ResultRows segmentToRows(QueryDataDecoder decoder, Segment segment)
    {
        if (segment instanceof InlineSegment) {
            InlineSegment inlineSegment = (InlineSegment) segment;
            try {
                return decoder.decode(new ByteArrayInputStream(inlineSegment.getData()), inlineSegment.getMetadata());
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        if (segment instanceof SpooledSegment) {
            SpooledSegment spooledSegment = (SpooledSegment) segment;

            try {
                InputStream stream = loader.load(spooledSegment);
                return decoder.decode(stream, spooledSegment.getMetadata());
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        throw new UnsupportedOperationException("Unsupported segment type: " + segment.getClass().getName());
    }

    public Optional<String> getEncoding()
    {
        return Optional.ofNullable(decoderFactory)
                .map(QueryDataDecoder.Factory::encoding);
    }

    @Override
    public void close()
            throws Exception
    {
        loader.close();
    }

    private static ResultRows concat(List<ResultRows> resultRows)
    {
        return () -> Iterators.concat(resultRows
                .stream()
                .filter(rows -> !rows.isNull())
                .map(ResultRows::iterator)
                .collect(toImmutableList())
                .iterator());
    }
}
