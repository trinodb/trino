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

import com.fasterxml.jackson.core.JsonParser;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingInputStream;
import io.trino.client.spooling.DataAttribute;
import io.trino.client.spooling.DataAttributes;
import io.trino.client.spooling.EncodedQueryData;
import io.trino.client.spooling.Segment;
import io.trino.client.spooling.SegmentLoader;
import io.trino.client.spooling.SpooledSegment;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static io.trino.client.JsonIterators.createJsonFactory;
import static io.trino.client.spooling.Segment.inlined;
import static io.trino.client.spooling.Segment.spooled;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

class TestResultRowsDecoder
{
    @Test
    public void testTypedNullMaterialization()
            throws Exception
    {
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            assertThat(decoder.toRows(fromQueryData(QueryData.NULL)))
                    .isEmpty();
        }
    }

    @Test
    public void testTypedJsonMaterialization()
            throws Exception
    {
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            assertThat(eagerlyMaterialize(decoder.toRows(fromQueryData(TypedQueryData.of(ImmutableList.of(ImmutableList.of(2137), ImmutableList.of(1337)))))))
                    .containsExactly(ImmutableList.of(2137), ImmutableList.of(1337));
        }
    }

    @Test
    public void testJsonNodeMaterialization()
            throws Exception
    {
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(); JsonParser parser = createJsonFactory().createParser("[[2137], [1337]]")) {
            assertThat(eagerlyMaterialize(decoder.toRows(fromQueryData(new JsonQueryData(parser.readValueAsTree())))))
                    .containsExactly(ImmutableList.of(2137), ImmutableList.of(1337));
        }
    }

    @Test
    public void testInlineJsonNodeMaterialization()
            throws Exception
    {
        try (ResultRowsDecoder decoder = new ResultRowsDecoder()) {
            assertThat(eagerlyMaterialize(decoder.toRows(fromSegments(inlined("[[2137], [1337]]".getBytes(UTF_8), DataAttributes.builder().set(DataAttribute.ROWS_COUNT, 2L).build())))))
                    .containsExactly(ImmutableList.of(2137), ImmutableList.of(1337));
        }
    }

    @Test
    public void testEagerInlineJsonNodeScanningMaterialization()
            throws Exception
    {
        CountingInputStream stream = new CountingInputStream(new ByteArrayInputStream("[[2137], [1337]]".getBytes(UTF_8)));
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(); JsonParser parser = createJsonFactory().createParser(stream)) {
            Iterator<List<Object>> iterator = decoder.toRows(fromQueryData(new JsonQueryData(parser.readValueAsTree()))).iterator();
            assertThat(stream.getCount()).isEqualTo(16);
            iterator.next();
            assertThat(stream.getCount()).isEqualTo(16);
        }
    }

    @Test
    public void testLazyInlineJsonNodeScanningMaterialization()
            throws Exception
    {
        CountingInputStream stream = new CountingInputStream(new ByteArrayInputStream("[[2137], [1337]]".getBytes(UTF_8)));
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(); JsonParser parser = createJsonFactory().createParser(stream)) {
            Iterator<List<Object>> iterator = decoder.toRows(fromQueryData(new JsonQueryData(parser.readValueAsTree()))).iterator();
            assertThat(stream.getCount()).isEqualTo(16);
            iterator.next();
            assertThat(stream.getCount()).isEqualTo(16);
        }
    }

    @Test
    public void testSpooledJsonMaterialization()
            throws Exception
    {
        AtomicInteger loaded = new AtomicInteger();
        AtomicInteger acknowledged = new AtomicInteger();
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(new StaticLoader(loaded, acknowledged))) {
            assertThat(eagerlyMaterialize(decoder.toRows(fromSegments(spooledSegment(2), spooledSegment(2)))))
                    .hasSize(4)
                    .containsExactly(ImmutableList.of(2137), ImmutableList.of(1337), ImmutableList.of(2137), ImmutableList.of(1337));
        }
        assertThat(loaded.get()).isEqualTo(2);
        assertThat(acknowledged.get()).isEqualTo(2);
    }

    @Test
    public void testSpooledJsonNodeMaterialization()
            throws Exception
    {
        AtomicInteger loaded = new AtomicInteger();
        AtomicInteger acknowledged = new AtomicInteger();
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(new StaticLoader(loaded, acknowledged))) {
            assertThat(eagerlyMaterialize(decoder.toRows(fromSegments(spooledSegment(2), spooledSegment(2)))))
                    .hasSize(4)
                    .containsExactly(ImmutableList.of(2137), ImmutableList.of(1337), ImmutableList.of(2137), ImmutableList.of(1337));
        }
        assertThat(loaded.get()).isEqualTo(2);
    }

    @Test
    public void testSpooledJsonNodeScanningMaterialization()
            throws Exception
    {
        String data = IntStream.range(0, 2500)
                .mapToObj(Integer::toString)
                .reduce("[", (a, b) -> a + "[" + b + "],", String::concat) + "[1337]]";
        CountingInputStream stream = new CountingInputStream(new ByteArrayInputStream(data.getBytes(UTF_8)));
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(loaderFromStream(stream))) {
            Iterator<List<Object>> iterator = decoder.toRows(fromSegments(spooledSegment(2501))).iterator();
            assertThat(stream.getCount()).isEqualTo(0);
            iterator.next();
            assertThat(stream.getCount()).isEqualTo(8000); // Jackson reads data in 8K chunks
            for (int i = 0; i < 1200; i++) {
                iterator.next();
            }
            assertThat(stream.getCount()).isEqualTo(8000);
            for (int i = 0; i < 1200; i++) {
                iterator.next();
            }
            assertThat(stream.getCount()).isEqualTo(16000);
            for (int i = 0; i < 100; i++) {
                iterator.next();
            }
            assertThat(stream.getCount()).isEqualTo(data.length());
        }
    }

    @Test
    public void testLazySpooledMaterialization()
            throws Exception
    {
        AtomicInteger loaded = new AtomicInteger();
        AtomicInteger acknowledged = new AtomicInteger();
        try (ResultRowsDecoder decoder = new ResultRowsDecoder(new StaticLoader(loaded, acknowledged))) {
            Iterator<List<Object>> iterator = decoder.toRows(fromSegments(spooledSegment(2), spooledSegment(2)))
                    .iterator();

            assertThat(loaded.get()).isEqualTo(0);
            assertThat(acknowledged.get()).isEqualTo(0);

            iterator.next(); // Half of the first segment consumed
            assertThat(loaded.get()).isEqualTo(1);
            assertThat(acknowledged.get()).isEqualTo(0);

            iterator.next(); // First segment fully consumed
            assertThat(loaded.get()).isEqualTo(1);
            assertThat(acknowledged.get()).isEqualTo(1);

            iterator.next(); // Half of the second segment consumed
            assertThat(loaded.get()).isEqualTo(2);
            assertThat(acknowledged.get()).isEqualTo(1);

            iterator.next(); // Second segment fully consumed
            assertThat(loaded.get()).isEqualTo(2);
            assertThat(acknowledged.get()).isEqualTo(2);

            assertThat(iterator.hasNext()).isFalse();
        }
    }

    private static class StaticLoader
            implements SegmentLoader
    {
        private final AtomicInteger loaded;
        private final AtomicInteger acknowledged;

        public StaticLoader(AtomicInteger loaded, AtomicInteger acknowledged)
        {
            this.loaded = requireNonNull(loaded, "loaded is null");
            this.acknowledged = requireNonNull(acknowledged, "acknowledged is null");
        }

        @Override
        public InputStream load(SpooledSegment segment)
        {
            loaded.incrementAndGet();
            return new ByteArrayInputStream("[[2137], [1337]]".getBytes(UTF_8));
        }

        @Override
        public void acknowledge(SpooledSegment segment)
        {
            acknowledged.incrementAndGet();
        }

        @Override
        public void close()
        {
        }
    }

    private static List<List<Object>> eagerlyMaterialize(Iterable<List<Object>> values)
    {
        return ImmutableList.copyOf(values);
    }

    private static SegmentLoader loaderFromStream(InputStream stream)
    {
        return new SegmentLoader() {
            @Override
            public InputStream load(SpooledSegment segment)
            {
                return stream;
            }

            @Override
            public void acknowledge(SpooledSegment segment)
            {
            }

            @Override
            public void close()
            {
            }
        };
    }

    private static QueryResults fromQueryData(QueryData queryData)
    {
        return new QueryResults(
                "id",
                URI.create("https://localhost"),
                URI.create("https://localhost"),
                URI.create("https://localhost"),
                ImmutableList.of(new Column("id", "integer", new ClientTypeSignature("integer", ImmutableList.of()))),
                queryData,
                StatementStats.builder()
                        .setState("FINISHED")
                        .setProgressPercentage(OptionalDouble.of(1.0))
                        .setRunningPercentage(OptionalDouble.of(0.0))
                        .build(),
                null,
                ImmutableList.of(),
                null,
                OptionalLong.of(0L));
    }

    private static QueryResults fromSegments(Segment... segments)
    {
        return fromQueryData(EncodedQueryData
                .builder("json")
                .withSegments(Arrays.asList(segments))
                .build());
    }

    private static Segment spooledSegment(long rows)
    {
        DataAttributes attributes = DataAttributes.builder()
                .set(DataAttribute.ROWS_COUNT, rows)
                .build();

        return spooled(URI.create("http://localhost"), URI.create("http://localhost"), attributes, ImmutableMap.of());
    }
}
