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

import com.google.common.collect.ImmutableMap;
import io.trino.client.CloseableIterator;
import io.trino.client.QueryDataDecoder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.List;

import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSpooledSegmentIterator
{
    private static final SpooledSegment SEGMENT = new SpooledSegment(
            URI.create("http://localhost/data"),
            URI.create("http://localhost/ack"),
            DataAttributes.builder().set(ROWS_COUNT, 1L).build(),
            ImmutableMap.of());

    @Test
    void testClosesStreamAndAcknowledgesSegmentWhenDecodingFails()
    {
        TrackingInputStream stream = new TrackingInputStream();
        RecordingSegmentLoader loader = new RecordingSegmentLoader(stream);
        IOException decodeFailure = new IOException("decode failed");

        SpooledSegmentIterator iterator = new SpooledSegmentIterator(SEGMENT, loader, failingDecoder(decodeFailure));

        assertThatThrownBy(iterator::load)
                .isInstanceOf(UncheckedIOException.class)
                .hasCause(decodeFailure);

        assertThat(stream.closed).isTrue();
        assertThat(loader.acknowledgedSegment).isEqualTo(SEGMENT);

        assertThatThrownBy(iterator::load)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Iterator is already closed");
    }

    @Test
    void testSuppressesCloseFailureWhenDecodingFails()
    {
        IOException closeFailure = new IOException("close failed");
        TrackingInputStream stream = new TrackingInputStream(closeFailure);
        RecordingSegmentLoader loader = new RecordingSegmentLoader(stream);
        IOException decodeFailure = new IOException("decode failed");

        SpooledSegmentIterator iterator = new SpooledSegmentIterator(SEGMENT, loader, failingDecoder(decodeFailure));

        assertThatThrownBy(iterator::load)
                .isInstanceOf(UncheckedIOException.class)
                .cause()
                .isSameAs(decodeFailure)
                .satisfies(cause -> assertThat(cause.getSuppressed()).contains(closeFailure));
    }

    private static QueryDataDecoder failingDecoder(IOException failure)
    {
        return new QueryDataDecoder()
        {
            @Override
            public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
                    throws IOException
            {
                throw failure;
            }

            @Override
            public String encoding()
            {
                return "failing";
            }
        };
    }

    private static class RecordingSegmentLoader
            implements SegmentLoader
    {
        private final InputStream stream;
        private SpooledSegment acknowledgedSegment;

        private RecordingSegmentLoader(InputStream stream)
        {
            this.stream = requireNonNull(stream, "stream is null");
        }

        @Override
        public InputStream load(SpooledSegment segment)
        {
            return stream;
        }

        @Override
        public void acknowledge(SpooledSegment segment)
        {
            this.acknowledgedSegment = requireNonNull(segment, "segment is null");
        }

        @Override
        public void close() {}
    }

    private static class TrackingInputStream
            extends InputStream
    {
        private final IOException closeFailure;
        private boolean closed;

        private TrackingInputStream()
        {
            this(null);
        }

        private TrackingInputStream(IOException closeFailure)
        {
            this.closeFailure = closeFailure;
        }

        @Override
        public int read()
        {
            return -1;
        }

        @Override
        public void close()
                throws IOException
        {
            closed = true;
            if (closeFailure != null) {
                throw closeFailure;
            }
        }
    }
}
