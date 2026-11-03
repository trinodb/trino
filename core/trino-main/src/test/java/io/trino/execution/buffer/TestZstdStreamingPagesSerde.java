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
package io.trino.execution.buffer;

import io.airlift.compress.v3.zstd.ZstdInputStream;
import io.airlift.compress.v3.zstd.ZstdNativeOutputStream;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.FeaturesConfig;
import io.trino.plugin.base.metrics.LongCount;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.execution.buffer.CompressionCodec.NONE;
import static io.trino.execution.buffer.CompressionCodec.ZSTD_STREAMING;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_HEADER_SIZE;
import static io.trino.execution.buffer.PagesSerdeUtil.SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET;
import static io.trino.execution.buffer.PagesSerdeUtil.readPages;
import static io.trino.execution.buffer.PagesSerdeUtil.writePages;
import static io.trino.execution.buffer.TestingPagesSerdes.createTestingPagesSerdeFactory;
import static io.trino.operator.PageAssertions.assertPageEquals;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.util.Ciphers.createRandomAesEncryptionKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestZstdStreamingPagesSerde
{
    @Test
    public void testConfiguration()
    {
        FeaturesConfig config = new ConfigurationFactory(Map.of("exchange.compression-codec", "ZSTD_STREAMING")).build(FeaturesConfig.class);
        assertThat(config.getExchangeCompressionCodec()).isEqualTo(ZSTD_STREAMING);
    }

    @Test
    public void testBackend()
    {
        String expectedNative = System.getProperty("exchange.test.native");
        if (expectedNative != null) {
            assertThat(ZstdNativeOutputStream.isEnabled()).isEqualTo(Boolean.parseBoolean(expectedNative));
        }
    }

    @Test
    public void testSingleFrameAndMetrics()
            throws IOException
    {
        Page page = randomPage();
        PageSerializer serializer = createTestingPagesSerdeFactory(ZSTD_STREAMING).createSerializer(Optional.empty());
        Slice serialized = serializer.serialize(page);
        Slice raw = createTestingPagesSerdeFactory(NONE).createSerializer(Optional.empty()).serialize(page);
        int uncompressedSize = raw.length() - SERIALIZED_PAGE_HEADER_SIZE;
        int compressedSize = serialized.length() - SERIALIZED_PAGE_HEADER_SIZE;
        assertThat(serialized.getInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET)).isEqualTo(uncompressedSize);
        assertThat(serialized.getInt(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET)).isEqualTo(compressedSize);
        // The payload is directly readable by the pure Java stream, with no exchange block markers.
        try (ZstdInputStream input = new ZstdInputStream(serialized.slice(SERIALIZED_PAGE_HEADER_SIZE, compressedSize).getInput())) {
            assertThat(input.readAllBytes()).isEqualTo(raw.slice(SERIALIZED_PAGE_HEADER_SIZE, uncompressedSize).getBytes());
        }
        serializer.serialize(page);
        assertThat(serializer.getAndResetMetrics().getMetrics()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "exchangeSerializerInputBytes", new LongCount(2L * uncompressedSize),
                "exchangeSerializerOutputBytes", new LongCount(2L * compressedSize)));
        assertThat(serializer.getMetrics().getMetrics()).containsExactlyInAnyOrderEntriesOf(Map.of(
                "exchangeSerializerInputBytes", new LongCount(0),
                "exchangeSerializerOutputBytes", new LongCount(0)));
    }

    @Test
    public void testIndependentEncryptedPages()
    {
        var key = Optional.of(createRandomAesEncryptionKey());
        PagesSerdeFactory factory = createTestingPagesSerdeFactory(ZSTD_STREAMING);
        PageSerializer serializer = factory.createSerializer(key);
        Page page = randomPage();
        Slice first = serializer.serialize(page);
        Slice second = serializer.serialize(page);
        // Each page gets a new IV and is independently decodable, including out of order.
        assertThat(first).isNotEqualTo(second);
        assertPageEquals(List.of(VARBINARY), factory.createDeserializer(key).deserialize(second), page);
        assertPageEquals(List.of(VARBINARY), factory.createDeserializer(key).deserialize(first), page);
    }

    @Test
    public void testPageFraming()
    {
        PagesSerdeFactory factory = createTestingPagesSerdeFactory(ZSTD_STREAMING);
        Page page = randomPage();
        var output = new DynamicSliceOutput(1024);
        writePages(factory.createSerializer(Optional.empty()), output, page, new Page(0), page);
        var pages = readPages(factory.createDeserializer(Optional.empty()), output.slice().getInput());
        assertPageEquals(List.of(VARBINARY), pages.next(), page);
        assertPageEquals(List.of(), pages.next(), new Page(0));
        assertPageEquals(List.of(VARBINARY), pages.next(), page);
        assertThat(pages.hasNext()).isFalse();
    }

    @Test
    public void testInvalidSizesAndTruncatedFrame()
    {
        PagesSerdeFactory factory = createTestingPagesSerdeFactory(ZSTD_STREAMING);
        PageDeserializer deserializer = factory.createDeserializer(Optional.empty());
        Slice serialized = factory.createSerializer(Optional.empty()).serialize(randomPage());
        Slice wrongSize = serialized.copy();
        wrongSize.setInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET, serialized.getInt(SERIALIZED_PAGE_UNCOMPRESSED_SIZE_OFFSET) + 1);
        assertThatThrownBy(() -> deserializer.deserialize(wrongSize))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid uncompressed page size");
        assertThatThrownBy(() -> deserializer.deserialize(serialized.slice(0, serialized.length() - 1)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Invalid compressed page size");
        Slice truncated = serialized.slice(0, serialized.length() - 1).copy();
        truncated.setInt(SERIALIZED_PAGE_COMPRESSED_SIZE_OFFSET, truncated.length() - SERIALIZED_PAGE_HEADER_SIZE);
        assertThatThrownBy(() -> deserializer.deserialize(truncated)).isInstanceOf(RuntimeException.class);
        // A failed operation does not retain a partially consumed stream.
        assertPageEquals(List.of(VARBINARY), deserializer.deserialize(serialized), randomPage());
    }

    private static Page randomPage()
    {
        Random random = new Random(42);
        BlockBuilder builder = VARBINARY.createBlockBuilder(null, 1024);
        byte[] bytes = new byte[1024];
        for (int position = 0; position < 1024; position++) {
            random.nextBytes(bytes);
            VARBINARY.writeSlice(builder, wrappedBuffer(bytes));
        }
        return new Page(builder.build());
    }
}
