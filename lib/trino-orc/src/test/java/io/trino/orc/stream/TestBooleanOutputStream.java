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

import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.orc.OrcOutputBuffer;
import io.trino.orc.checkpoint.BooleanStreamCheckpoint;
import io.trino.orc.checkpoint.ByteStreamCheckpoint;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.orc.metadata.CompressionKind.NONE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestBooleanOutputStream
{
    @Test
    public void testWriteBoolean()
    {
        List<List<Integer>> testGroups = ImmutableList.of(
                ImmutableList.of(149, 317, 2),
                ImmutableList.of(2),
                ImmutableList.of(1, 2, 4, 0, 8),
                ImmutableList.of(1, 4, 8, 1024, 10000),
                ImmutableList.of(14000, 1, 2));

        for (List<Integer> counts : testGroups) {
            OrcOutputBuffer buffer = new OrcOutputBuffer(NONE, 1024);
            BooleanOutputStream output = new BooleanOutputStream(buffer);

            // write multiple booleans together
            for (int count : counts) {
                output.writeBooleans(count, true);
                output.recordCheckpoint();
            }
            output.close();

            List<BooleanStreamCheckpoint> batchWriteCheckpoints = output.getCheckpoints();
            DynamicSliceOutput slice = new DynamicSliceOutput(128);
            buffer.writeDataTo(slice);
            Slice batchWriteBuffer = slice.slice();

            // write one boolean a time
            buffer.reset();
            output.reset();
            for (int count : counts) {
                for (int i = 0; i < count; i++) {
                    output.writeBoolean(true);
                }
                output.recordCheckpoint();
            }
            output.close();
            List<BooleanStreamCheckpoint> singleWriteCheckpoints = output.getCheckpoints();
            slice = new DynamicSliceOutput(128);
            buffer.writeDataTo(slice);
            Slice singleWriteBuffer = slice.slice();

            assertThat(batchWriteCheckpoints).hasSize(singleWriteCheckpoints.size());
            for (int i = 0; i < batchWriteCheckpoints.size(); i++) {
                assertThat(checkpointsEqual(batchWriteCheckpoints.get(i), singleWriteCheckpoints.get(i))).isTrue();
            }
            assertThat(batchWriteBuffer).isEqualTo(singleWriteBuffer);
        }
    }

    private static boolean checkpointsEqual(BooleanStreamCheckpoint left, BooleanStreamCheckpoint right)
    {
        assertThat(left).isNotNull();
        assertThat(right).isNotNull();
        if (left.getOffset() != right.getOffset()) {
            return false;
        }

        ByteStreamCheckpoint leftCheckpoint = left.getByteStreamCheckpoint();
        ByteStreamCheckpoint rightCheckpoint = right.getByteStreamCheckpoint();
        assertThat(leftCheckpoint).isNotNull();
        assertThat(rightCheckpoint).isNotNull();

        return leftCheckpoint.getInputStreamCheckpoint() == rightCheckpoint.getInputStreamCheckpoint() &&
                leftCheckpoint.getOffset() == rightCheckpoint.getOffset();
    }
}
