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
package io.trino.operator;

import org.junit.jupiter.api.Test;

import static io.trino.operator.OutputSpoolingController.Mode.BUFFER;
import static io.trino.operator.OutputSpoolingController.Mode.INLINE;
import static io.trino.operator.OutputSpoolingController.Mode.SPOOL;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

class TestOutputSpoolingController
{
    @Test
    public void testInlineFirstRowsUntilThresholdThenSpooling()
    {
        var assertion = new OutputSpoolingControllerAssertions(
                new OutputSpoolingController(true, 100, 1000, 900, 16000));

        assertion
                .verifyNextMode(10, 100, INLINE)
                .verifyInlined(1, 10, 100)
                .verifyNextMode(10, 100, INLINE)
                .verifyInlined(2, 20, 200)
                .verifyNextMode(50, 400, INLINE)
                .verifyInlined(3, 70, 600)
                .verifyNextMode(50, 400, BUFFER)
                .verifyBuffered(50, 400)
                .verifyNextMode(50, 400, BUFFER)
                .verifyBuffered(100, 800)
                .verifyNextMode(50, 400, SPOOL)
                .verifySpooled(1, 150, 1200)
                .verifyEmptyBuffer()
                .verifyNextMode(39, 399, BUFFER)
                .verifyBuffered(39, 399);
    }

    @Test
    public void testSpoolingTargetSize()
    {
        var assertion = new OutputSpoolingControllerAssertions(
                new OutputSpoolingController(false, 0, 0, 512, 2048));

        assertion
                .verifyNextMode(100, 511, BUFFER) // still under the initial segment target
                .verifySpooledSegmentTarget(512)
                .verifyBuffered(100, 511)
                .verifyNextMode(100, 1, SPOOL)
                .verifySpooled(1, 200, 512)
                .verifySpooledSegmentTarget(1024) // target doubles
                .verifyEmptyBuffer()
                .verifyNextMode(1, 333, BUFFER)
                .verifyNextMode(1, 333, BUFFER)
                .verifyNextMode(1, 333, BUFFER)
                .verifyNextMode(1, 333, SPOOL)
                .verifySpooled(2, 204, 512 + 333 * 4)
                .verifyEmptyBuffer()
                .verifySpooledSegmentTarget(2048) // target doubled again
                .verifyNextMode(100, 2047, BUFFER)
                .verifyNextMode(100, 2047, SPOOL)
                .verifyEmptyBuffer()
                .verifySpooledSegmentTarget(2048) // target clamped at max
                .verifySpooled(3, 204 + 200, 512 + 333 * 4 + 2047 * 2);
    }

    @Test
    public void testSpoolingEncoderEfficiency()
    {
        var assertion = new OutputSpoolingControllerAssertions(
                new OutputSpoolingController(false, 0, 0, 32, 100));

        assertion
                .verifyNextMode(1000, 31, BUFFER)
                .verifyBuffered(1000, 31)
                .verifyNextMode(1000, 31, SPOOL)
                .verifySpooled(1, 2000, 62)
                .recordEncodedSize(31)
                .verifyEmptyBuffer()
                .verifySpooledSegmentTarget(64)
                .verifyNextMode(100, 80, SPOOL)
                .verifyNextMode(100, 47, BUFFER) // over segment size
                .verifySpooled(2, 2100, 142)
                .verifyBuffered(100, 47)
                .verifyNextMode(54, 1, BUFFER)
                .recordEncodedSize(121)
                .verifySpooledSegmentTarget(100)
                .verifyNextMode(100, 80, SPOOL)
                .verifyNextMode(100, 43, BUFFER)
                .verifyNextMode(1, 1, BUFFER)
                .verifyNextMode(100, 1, BUFFER)
                .verifyNextMode(100, 80, SPOOL)
                .verifyEmptyBuffer()
                .verifySpooled(4, 2655, 395);
    }

    private record OutputSpoolingControllerAssertions(OutputSpoolingController controller)
    {
        public OutputSpoolingControllerAssertions verifyNextMode(int positionCount, int rawSizeInBytes, OutputSpoolingController.Mode expected)
        {
            assertThat(controller.getNextMode(positionCount, rawSizeInBytes))
                    .isEqualTo(expected);

            return this;
        }

        private OutputSpoolingControllerAssertions verifyInlined(int inlinedPages, int inlinedPositions, int inlinedRawBytes)
        {
            assertThat(controller.getInlinedPages())
                    .describedAs("Inlined pages")
                    .isEqualTo(inlinedPages);

            assertThat(controller.getInlinedPositions())
                    .describedAs("Inlined positions")
                    .isEqualTo(inlinedPositions);

            assertThat(controller.getInlinedRawBytes())
                    .describedAs("Inlined raw bytes")
                    .isEqualTo(inlinedRawBytes);

            return this;
        }

        private OutputSpoolingControllerAssertions verifySpooled(int spooledPages, int spooledPositions, int spooledRawBytes)
        {
            assertThat(controller.getSpooledPages())
                    .describedAs("Spooled pages")
                    .isEqualTo(spooledPages);

            assertThat(controller.getSpooledPositions())
                    .describedAs("Spooled spooledPositions")
                    .isEqualTo(spooledPositions);

            assertThat(controller.getSpooledRawBytes())
                    .describedAs("Spooled raw bytes")
                    .isEqualTo(spooledRawBytes);

            return this;
        }

        private OutputSpoolingControllerAssertions verifyBuffered(int bufferedPositions, int bufferSize)
        {
            assertThat(controller.getBufferedPositions())
                    .describedAs("Buffered positions")
                    .isEqualTo(bufferedPositions);
            assertThat(controller.getBufferedRawSize())
                    .describedAs("Buffered size")
                    .isEqualTo(bufferSize);

            return this;
        }

        private OutputSpoolingControllerAssertions verifySpooledSegmentTarget(long size)
        {
            assertThat(controller.getCurrentSpooledSegmentTarget())
                    .describedAs("Spooled segment target")
                    .isEqualTo(size);

            return this;
        }

        private OutputSpoolingControllerAssertions recordEncodedSize(long encodedSize)
        {
            controller.recordEncoded(encodedSize);
            return this;
        }

        private OutputSpoolingControllerAssertions verifyEmptyBuffer()
        {
            return this.verifyBuffered(0, 0);
        }
    }
}
