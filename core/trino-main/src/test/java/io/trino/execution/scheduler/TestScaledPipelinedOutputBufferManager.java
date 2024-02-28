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
import org.junit.jupiter.api.Test;

import static io.trino.execution.buffer.PipelinedOutputBuffers.BufferType.ARBITRARY;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link ScaledPipelinedOutputBufferManager}.
 */
public class TestScaledPipelinedOutputBufferManager
{
    @Test
    public void test()
    {
        ScaledPipelinedOutputBufferManager scaledPipelinedOutputBufferManager = new ScaledPipelinedOutputBufferManager();
        assertThat(scaledPipelinedOutputBufferManager.getOutputBuffers()).isEqualTo(PipelinedOutputBuffers.createInitial(ARBITRARY));

        scaledPipelinedOutputBufferManager.addOutputBuffer(new PipelinedOutputBuffers.OutputBufferId(0));
        PipelinedOutputBuffers expectedOutputBuffers = PipelinedOutputBuffers.createInitial(ARBITRARY).withBuffer(new PipelinedOutputBuffers.OutputBufferId(0), 0);
        assertThat(scaledPipelinedOutputBufferManager.getOutputBuffers()).isEqualTo(expectedOutputBuffers);

        scaledPipelinedOutputBufferManager.addOutputBuffer(new PipelinedOutputBuffers.OutputBufferId(1));
        scaledPipelinedOutputBufferManager.addOutputBuffer(new PipelinedOutputBuffers.OutputBufferId(2));

        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new PipelinedOutputBuffers.OutputBufferId(1), 1);
        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new PipelinedOutputBuffers.OutputBufferId(2), 2);
        assertThat(scaledPipelinedOutputBufferManager.getOutputBuffers()).isEqualTo(expectedOutputBuffers);

        // set no more buffers
        scaledPipelinedOutputBufferManager.addOutputBuffer(new PipelinedOutputBuffers.OutputBufferId(3));
        scaledPipelinedOutputBufferManager.noMoreBuffers();
        expectedOutputBuffers = expectedOutputBuffers.withBuffer(new PipelinedOutputBuffers.OutputBufferId(3), 3);
        expectedOutputBuffers = expectedOutputBuffers.withNoMoreBufferIds();
        assertThat(scaledPipelinedOutputBufferManager.getOutputBuffers()).isEqualTo(expectedOutputBuffers);

        // try to add another buffer, which should not result in an error
        // and output buffers should not change
        scaledPipelinedOutputBufferManager.addOutputBuffer(new PipelinedOutputBuffers.OutputBufferId(5));
        assertThat(scaledPipelinedOutputBufferManager.getOutputBuffers()).isEqualTo(expectedOutputBuffers);

        // try to set no more buffers again, which should not result in an error
        // and output buffers should not change
        scaledPipelinedOutputBufferManager.noMoreBuffers();
        assertThat(scaledPipelinedOutputBufferManager.getOutputBuffers()).isEqualTo(expectedOutputBuffers);
    }
}
