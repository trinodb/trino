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
package io.trino.operator.window;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.ValueWindowFunction;
import io.trino.spi.function.WindowFunctionSignature;

import java.util.List;

import static com.google.common.collect.Iterables.getOnlyElement;

@WindowFunctionSignature(name = "last_value", typeVariable = "T", returnType = "T", argumentTypes = "T")
public class LastValueFunction
        extends ValueWindowFunction
{
    private final int argumentChannel;
    private final boolean ignoreNulls;

    // Record a frame end with corresponding first non-null position
    // or last seen null position (going backwards).
    // The non-null position is valid for all frames starting from the recorded non-null position or before
    // and ending at the recorded frame end or before.
    private int recordedFrameEnd = -1;
    private int recordedValuePosition = -1;
    private int recordedNullPosition = -1;

    public LastValueFunction(List<Integer> argumentChannels, boolean ignoreNulls)
    {
        this.argumentChannel = getOnlyElement(argumentChannels);
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    public void reset()
    {
        recordedFrameEnd = -1;
        recordedValuePosition = -1;
        recordedNullPosition = -1;
    }

    @Override
    public void processRow(BlockBuilder output, int frameStart, int frameEnd, int currentPosition)
    {
        // empty frame
        if (frameStart < 0) {
            output.appendNull();
            return;
        }

        if (!ignoreNulls) {
            windowIndex.appendTo(argumentChannel, frameEnd, output);
            return;
        }

        if (recordedFrameEnd >= 0 && frameEnd <= recordedFrameEnd) {
            // try to use the recorded non-null position
            if (recordedValuePosition >= 0 && frameEnd >= recordedValuePosition) {
                if (frameStart > recordedValuePosition) {
                    // only nulls in the frame
                    output.appendNull();
                    return;
                }
                // use the recorded non-null position
                windowIndex.appendTo(argumentChannel, recordedValuePosition, output);
                return;
            }
            // try to use the recorded null position
            if (recordedNullPosition >= 0 && frameEnd >= recordedNullPosition) {
                if (frameStart >= recordedNullPosition) {
                    // only nulls in the frame
                    output.appendNull();
                    return;
                }
            }
        }

        // there is no recorded position or the current frame is before / after the recorded frame
        // find the first non-null or last null position for current the frame and record it for future reference
        // try to optimize by using the recorded null position
        int valuePosition;
        if (recordedFrameEnd >= 0 && recordedNullPosition >= 0 && frameEnd <= recordedFrameEnd && frameEnd >= recordedNullPosition) {
            valuePosition = recordedNullPosition;
        }
        else {
            valuePosition = frameEnd;
        }

        recordedFrameEnd = frameEnd;
        recordedValuePosition = -1;
        recordedNullPosition = -1;

        while (valuePosition >= frameStart) {
            if (!windowIndex.isNull(argumentChannel, valuePosition)) {
                break;
            }

            valuePosition--;
        }

        if (valuePosition < frameStart) {
            recordedNullPosition = frameStart;
            output.appendNull();
            return;
        }

        recordedValuePosition = valuePosition;
        windowIndex.appendTo(argumentChannel, valuePosition, output);
    }
}
