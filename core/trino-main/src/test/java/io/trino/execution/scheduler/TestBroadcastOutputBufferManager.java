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

import io.trino.execution.buffer.OutputBuffers;
import io.trino.execution.buffer.OutputBuffers.OutputBufferId;
import org.testng.annotations.Test;

import static io.trino.execution.buffer.OutputBuffers.BROADCAST_PARTITION_ID;
import static io.trino.execution.buffer.OutputBuffers.BufferType.BROADCAST;
import static io.trino.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static org.testng.Assert.assertEquals;

public class TestBroadcastOutputBufferManager
{
    @Test
    public void test()
    {
        BroadcastOutputBufferManager hashOutputBufferManager = new BroadcastOutputBufferManager();
        assertEquals(hashOutputBufferManager.getOutputBuffers(), createInitialEmptyOutputBuffers(BROADCAST));

        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(0));
        OutputBuffers expectedOutputBuffers = createInitialEmptyOutputBuffers(BROADCAST).withBuffer(new OutputBufferId(0), BROADCAST_PARTITION_ID);
        assertEquals(hashOutputBufferManager.getOutputBuffers(), expectedOutputBuffers);

        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(1));
        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(2));

        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new OutputBufferId(1), BROADCAST_PARTITION_ID);
        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new OutputBufferId(2), BROADCAST_PARTITION_ID);
        assertEquals(hashOutputBufferManager.getOutputBuffers(), expectedOutputBuffers);

        // set no more buffers
        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(3));
        hashOutputBufferManager.noMoreBuffers();
        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new OutputBufferId(3), BROADCAST_PARTITION_ID);
        expectedOutputBuffers = expectedOutputBuffers.withNoMoreBufferIds();
        assertEquals(hashOutputBufferManager.getOutputBuffers(), expectedOutputBuffers);

        // try to add another buffer, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(5));
        assertEquals(hashOutputBufferManager.getOutputBuffers(), expectedOutputBuffers);

        // try to set no more buffers again, which should not result in an error
        // and output buffers should not change
        hashOutputBufferManager.addOutputBuffer(new OutputBufferId(6));
        assertEquals(hashOutputBufferManager.getOutputBuffers(), expectedOutputBuffers);
    }
}
