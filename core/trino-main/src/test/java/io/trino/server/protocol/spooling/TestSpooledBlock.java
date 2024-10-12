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
package io.trino.server.protocol.spooling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.trino.client.spooling.DataAttributes;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.server.protocol.spooling.SpooledBlock.SPOOLING_METADATA_TYPE;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class TestSpooledBlock
{
    @Test
    public void testSerializationRoundTrip()
    {
        verifySerialization(utf8Slice("identifier"), Optional.of(URI.create("https://example.com/presigned")), ImmutableMap.of("X-Header", ImmutableList.of("value1", "value2")));
        verifySerialization(utf8Slice("identifier"), Optional.empty(), ImmutableMap.of("X-Header", ImmutableList.of("value1", "value2")));
    }

    public void verifySerialization(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        verifySerializationRoundTrip(identifier, directUri, headers);
        verifySerializationRoundTripWithNonEmptyPage(identifier, directUri, headers);
        verifyThrowsErrorOnNonNullPositions(identifier, directUri, headers);
        verifyThrowsErrorOnMultiplePositions(identifier, directUri, headers);
    }

    public void verifySerializationRoundTrip(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        SpooledBlock metadata = new SpooledBlock(identifier, directUri, headers, createDataAttributes(10, 1200));
        Page page = new Page(metadata.serialize());
        SpooledBlock retrieved = SpooledBlock.deserialize(page);
        assertThat(metadata).isEqualTo(retrieved);
    }

    private void verifySerializationRoundTripWithNonEmptyPage(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        SpooledBlock metadata = new SpooledBlock(identifier, directUri, headers, createDataAttributes(10, 1100));
        Page page = new Page(blockWithPositions(1, true), metadata.serialize());
        SpooledBlock retrieved = SpooledBlock.deserialize(page);
        assertThat(metadata).isEqualTo(retrieved);
    }

    private void verifyThrowsErrorOnNonNullPositions(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        SpooledBlock metadata = new SpooledBlock(identifier, directUri, headers, createDataAttributes(20, 1200));

        assertThatThrownBy(() -> SpooledBlock.deserialize(new Page(blockWithPositions(1, false), metadata.serialize())))
                .hasMessage("Spooling metadata block must have all but last channels null");
    }

    private void verifyThrowsErrorOnMultiplePositions(Slice identifier, Optional<URI> directUri, Map<String, List<String>> headers)
    {
        SpooledBlock metadata = new SpooledBlock(identifier, directUri, headers, createDataAttributes(30, 1300));
        RowBlockBuilder rowBlockBuilder = SPOOLING_METADATA_TYPE.createBlockBuilder(null, 2);
        metadata.serialize(rowBlockBuilder);
        metadata.serialize(rowBlockBuilder);
        assertThatThrownBy(() -> SpooledBlock.deserialize(new Page(blockWithPositions(2, false), rowBlockBuilder.build())))
                .hasMessage("Spooling metadata block must have a single position");
    }

    public static Block blockWithPositions(int count, boolean isNull)
    {
        BlockBuilder blockBuilder = BIGINT.createFixedSizeBlockBuilder(count);
        for (int i = 0; i < count; i++) {
            if (isNull) {
                blockBuilder.appendNull();
            }
            else {
                BIGINT.writeLong(blockBuilder, 0);
            }
        }
        return blockBuilder.build();
    }

    private static DataAttributes createDataAttributes(long rows, int segmentSize)
    {
        return DataAttributes.builder()
                .set(ROWS_COUNT, rows)
                .set(SEGMENT_SIZE, segmentSize)
                .build();
    }
}
