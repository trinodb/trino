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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.FeaturesConfig;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;

import static io.trino.execution.buffer.PagesSerdeUtil.calculateChecksum;
import static io.trino.server.PagesInputStreamFactory.SERIALIZED_PAGES_MAGIC;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPagesInputStreamFactory
{
    @Test
    public void testWritePages()
            throws IOException
    {
        List<Slice> pages = ImmutableList.of(
                Slices.utf8Slice("first page"),
                Slices.wrappedBuffer(new byte[] {1, 2, 3, 4}));

        TestingOutputStream stream = new TestingOutputStream();
        new PagesInputStreamFactory(new FeaturesConfig()).write(stream, pages);

        DynamicSliceOutput expected = new DynamicSliceOutput(64);
        expected.writeInt(SERIALIZED_PAGES_MAGIC);
        expected.writeLong(calculateChecksum(pages));
        expected.writeInt(pages.size());
        pages.forEach(expected::writeBytes);

        assertThat(stream.toByteArray()).isEqualTo(expected.slice().getBytes());
        assertThat(stream.flushCount()).isEqualTo(1);
        assertThat(stream.closed()).isTrue();
    }

    private static class TestingOutputStream
            extends ByteArrayOutputStream
    {
        private int flushCount;
        private boolean closed;

        @Override
        public void flush()
        {
            flushCount++;
        }

        @Override
        public void close()
        {
            closed = true;
        }

        public int flushCount()
        {
            return flushCount;
        }

        public boolean closed()
        {
            return closed;
        }
    }
}
