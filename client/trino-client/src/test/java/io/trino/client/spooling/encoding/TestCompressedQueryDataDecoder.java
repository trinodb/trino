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
package io.trino.client.spooling.encoding;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import io.trino.client.CloseableIterator;
import io.trino.client.QueryDataDecoder;
import io.trino.client.spooling.DataAttributes;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.trino.client.CloseableIterator.closeable;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.client.spooling.DataAttribute.UNCOMPRESSED_SIZE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

class TestCompressedQueryDataDecoder
{
    private static final List<List<Object>> SAMPLE_VALUES = ImmutableList.of(ImmutableList.of("hello", "world"));

    @Test
    public void testClosesUnderlyingInputStreamIfCompressed()
            throws IOException
    {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream stream = new FilterInputStream(new ByteArrayInputStream("compressed".getBytes(UTF_8))) {
            @Override
            public void close()
                    throws IOException
            {
                super.close();
                closed.set(true);
            }
        };

        QueryDataDecoder decoder = new TestQueryDataDecoder(new QueryDataDecoder() {
            @Override
            public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
                    throws IOException
            {
                assertThat(new String(ByteStreams.toByteArray(input), UTF_8))
                        .isEqualTo("decompressed");
                return closeable(SAMPLE_VALUES.iterator());
            }

            @Override
            public String encoding()
            {
                return "test";
            }
        });

        assertThat(closed.get()).isFalse();
        assertThat(decoder.decode(stream, DataAttributes
                .builder()
                .set(UNCOMPRESSED_SIZE, "decompressed".length())
                .set(SEGMENT_SIZE, "compressed".length())
                .build()))
                .toIterable()
                .containsAll(SAMPLE_VALUES);
        assertThat(closed.get()).isTrue();
    }

    @Test
    public void testDelegatesClosingIfUncompressed()
            throws IOException
    {
        AtomicBoolean closed = new AtomicBoolean();
        InputStream stream = new FilterInputStream(new ByteArrayInputStream("not compressed".getBytes(UTF_8))) {
            @Override
            public void close()
                    throws IOException
            {
                super.close();
                closed.set(true);
            }
        };

        QueryDataDecoder decoder = new TestQueryDataDecoder(new QueryDataDecoder() {
            @Override
            public CloseableIterator<List<Object>> decode(InputStream input, DataAttributes segmentAttributes)
                    throws IOException
            {
                assertThat(new String(ByteStreams.toByteArray(input), UTF_8))
                        .isEqualTo("not compressed");
                input.close(); // Closes input stream according to the contract
                return closeable(SAMPLE_VALUES.iterator());
            }

            @Override
            public String encoding()
            {
                return "test";
            }
        });

        assertThat(closed.get()).isFalse();
        assertThat(decoder.decode(stream, DataAttributes.builder()
                .set(SEGMENT_SIZE, "not compressed".length())
                .build()))
                .toIterable()
                .containsAll(SAMPLE_VALUES);
        assertThat(closed.get()).isTrue();
    }

    private static class TestQueryDataDecoder
            extends CompressedQueryDataDecoder
    {
        public TestQueryDataDecoder(QueryDataDecoder delegate)
        {
            super(delegate);
        }

        @Override
        void decompress(byte[] bytes, byte[] output)
        {
            assertThat(new String(bytes, UTF_8))
                    .isEqualTo("compressed");

            byte[] uncompressed = "decompressed".getBytes(UTF_8);
            System.arraycopy(uncompressed, 0, output, 0, uncompressed.length);
        }

        @Override
        public String encoding()
        {
            return "test";
        }
    }
}
