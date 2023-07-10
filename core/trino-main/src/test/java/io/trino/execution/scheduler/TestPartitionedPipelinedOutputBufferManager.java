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
package io.trino.execution.scheduler;

import io.trino.execution.buffer.PipelinedOutputBuffers;
import io.trino.execution.buffer.PipelinedOutputBuffers.OutputBufferId;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPartitionedPipelinedOutputBufferManager
{
    @Test
    public void test()
    {
        PartitionedPipelinedOutputBufferManager hashOutputBufferManager = new PartitionedPipelinedOutputBufferManager(FIXED_HASH_DISTRIBUTION, 4);

        // output buffers are set immediately when the manager is created
        assertOutputBuffers(hashOutputBufferManager.getOutputBuffers());

        // add buffers, which does not cause an error
        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(0));
        assertOutputBuffers(hashOutputBufferManager.getOutputBuffers());
        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(3));
        assertOutputBuffers(hashOutputBufferManager.getOutputBuffers());

        // try to a buffer out side of the partition range, which should result in an error
        assertThatThrownBy(() -> hashOutputBufferManager.addOutputBuffer(new OutputBufferId(5)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Unexpected new output buffer 5");
        assertOutputBuffers(hashOutputBufferManager.getOutputBuffers());

        // try to set no more buffers again, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.noMoreBuffers();
        assertOutputBuffers(hashOutputBufferManager.getOutputBuffers());
    }

    private static void assertOutputBuffers(PipelinedOutputBuffers outputBuffers)
    {
        assertNotNull(outputBuffers);
        assertTrue(outputBuffers.getVersion() > 0);
        assertTrue(outputBuffers.isNoMoreBufferIds());
        Map<OutputBufferId, Integer> buffers = outputBuffers.getBuffers();
        assertEquals(buffers.size(), 4);
        for (int partition = 0; partition < 4; partition++) {
            assertEquals(buffers.get(new OutputBufferId(partition)), Integer.valueOf(partition));
        }
    }
}
