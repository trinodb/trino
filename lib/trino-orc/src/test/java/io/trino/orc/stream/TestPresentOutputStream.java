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
package io.trino.orc.stream;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.orc.metadata.OrcColumnId;
import io.trino.spi.block.BitArrayBlock;
import io.trino.spi.block.Block;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.orc.metadata.CompressionKind.NONE;
import static io.trino.spi.block.Bitmap.allocateWords;
import static io.trino.spi.block.Bitmap.set;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPresentOutputStream
{
    private static final OrcColumnId COLUMN_ID = new OrcColumnId(33);

    @Test
    public void testWriteBlock()
    {
        assertWriteBlockMatchesScalar(List.of(
                createBlock(0, 1, 0),
                createBlock(1, 7, 3),
                createBlock(7, 64, 5),
                createBlock(63, 65, 11),
                createBlock(5, 127, 17)));
    }

    @Test
    public void testNullAfterAllValidGroups()
    {
        assertWriteBlockMatchesScalar(List.of(
                createBlock(3, 17, -1),
                createBlock(7, 70, -1),
                createBlock(13, 129, 67)));
    }

    @Test
    public void testAllValidBlocksDoNotCreatePresentStream()
    {
        PresentOutputStream output = new PresentOutputStream(NONE, 1024);
        output.recordCheckpoint();
        assertThat(output.writeBlock(createBlock(7, 129, -1))).isEqualTo(129);
        output.close();

        assertThat(output.getCheckpoints()).isEmpty();
        assertThat(output.getStreamDataOutput(COLUMN_ID)).isEmpty();
    }

    private static void assertWriteBlockMatchesScalar(List<Block> blocks)
    {
        PresentOutputStream packed = new PresentOutputStream(NONE, 1024);
        PresentOutputStream scalar = new PresentOutputStream(NONE, 1024);

        for (Block block : blocks) {
            packed.recordCheckpoint();
            scalar.recordCheckpoint();

            int expectedNonNullCount = 0;
            for (int position = 0; position < block.getPositionCount(); position++) {
                boolean present = !block.isNull(position);
                scalar.writeBoolean(present);
                expectedNonNullCount += present ? 1 : 0;
            }
            assertThat(packed.writeBlock(block)).isEqualTo(expectedNonNullCount);
        }

        packed.close();
        scalar.close();

        assertThat(readData(packed)).isEqualTo(readData(scalar));
        assertThat(checkpointPositions(packed)).isEqualTo(checkpointPositions(scalar));
    }

    private static Optional<Slice> readData(PresentOutputStream output)
    {
        return output.getStreamDataOutput(COLUMN_ID)
                .map(stream -> {
                    DynamicSliceOutput sliceOutput = new DynamicSliceOutput(128);
                    stream.writeData(sliceOutput);
                    return sliceOutput.slice();
                });
    }

    private static Optional<List<List<Integer>>> checkpointPositions(PresentOutputStream output)
    {
        return output.getCheckpoints()
                .map(checkpoints -> checkpoints.stream()
                        .map(checkpoint -> checkpoint.toPositionList(false))
                        .toList());
    }

    private static BitArrayBlock createBlock(int rawBitOffset, int positionCount, int nullPosition)
    {
        int rawPositionCount = rawBitOffset + positionCount;
        long[] values = allocateWords(rawPositionCount, false);
        long[] validity = nullPosition < 0 ? null : allocateWords(rawPositionCount, false);
        if (validity != null) {
            for (int position = 0; position < positionCount; position++) {
                if (position != nullPosition) {
                    set(validity, rawBitOffset, position);
                }
            }
        }
        return new BitArrayBlock(rawPositionCount, Optional.ofNullable(validity), values).getRegion(rawBitOffset, positionCount);
    }
}
