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
package io.trino.operator.spooling;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.client.spooling.DataAttributes;
import io.trino.server.protocol.spooling.SpooledBlock;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.protocol.SpooledLocation;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.client.spooling.DataAttribute.ROWS_COUNT;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.spi.protocol.SpooledLocation.coordinatorLocation;
import static io.trino.spi.protocol.SpooledLocation.directLocation;
import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

class TestSpooledBlock
{
    @Test
    public void testSerializationRoundTrip()
    {
        verifySerialization(directLocation(URI.create("https://example.com/presigned"), ImmutableMap.of("X-Header", ImmutableList.of("value1", "value2"))));
        verifySerialization(coordinatorLocation(utf8Slice("identifier"), ImmutableMap.of("X-Header", ImmutableList.of("value1", "value2"))));
    }

    public void verifySerialization(SpooledLocation location)
    {
        verifySerializationRoundTrip(location);
        verifySerializationRoundTripWithNonEmptyPage(location);
        verifyThrowsErrorOnNonNullPositions(location);
        verifyThrowsErrorOnMultiplePositions(location);
    }

    public void verifySerializationRoundTrip(SpooledLocation location)
    {
        SpooledBlock metadata = new SpooledBlock(location, createDataAttributes(10, 1200));
        Page page = new Page(metadata.serialize());
        SpooledBlock retrieved = SpooledBlock.deserialize(page);
        assertThat(metadata).isEqualTo(retrieved);
    }

    private void verifySerializationRoundTripWithNonEmptyPage(SpooledLocation location)
    {
        SpooledBlock metadata = new SpooledBlock(location, createDataAttributes(10, 1100));
        Page page = new Page(blockWithPositions(1, true), metadata.serialize());
        SpooledBlock retrieved = SpooledBlock.deserialize(page);
        assertThat(metadata).isEqualTo(retrieved);
    }

    private void verifyThrowsErrorOnNonNullPositions(SpooledLocation location)
    {
        SpooledBlock metadata = new SpooledBlock(location, createDataAttributes(20, 1200));

        assertThatThrownBy(() -> SpooledBlock.deserialize(new Page(blockWithPositions(1, false), metadata.serialize())))
                .hasMessage("Spooling metadata block must have all but last channels null");
    }

    private void verifyThrowsErrorOnMultiplePositions(SpooledLocation location)
    {
        SpooledBlock metadata = new SpooledBlock(location, createDataAttributes(30, 1300));

        assertThatThrownBy(() -> SpooledBlock.deserialize(new Page(blockWithPositions(2, false), metadata.serialize())))
                .hasMessage("Spooling metadata block must have a single position");
    }

    public static Block blockWithPositions(int count, boolean isNull)
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, count);
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
