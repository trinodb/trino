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

@WindowFunctionSignature(name = "first_value", typeVariable = "T", returnType = "T", argumentTypes = "T")
public class FirstValueFunction
        extends ValueWindowFunction
{
    private final int argumentChannel;
    private final boolean ignoreNulls;

    // Record a frame start with corresponding first non-null position
    // or last seen null position.
    // The non-null position is valid for all frames starting from the recorded frame start or later,
    // and ending at the recorded non-null position or later.
    private int recordedFrameStart = -1;
    private int recordedValuePosition = -1;
    private int recordedNullPosition = -1;

    public FirstValueFunction(List<Integer> argumentChannels, boolean ignoreNulls)
    {
        this.argumentChannel = getOnlyElement(argumentChannels);
        this.ignoreNulls = ignoreNulls;
    }

    @Override
    public void reset()
    {
        recordedFrameStart = -1;
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
            windowIndex.appendTo(argumentChannel, frameStart, output);
            return;
        }

        if (recordedFrameStart >= 0 && frameStart >= recordedFrameStart) {
            // try to use the recorded non-null position
            if (recordedValuePosition >= 0 && frameStart <= recordedValuePosition) {
                if (frameEnd < recordedValuePosition) {
                    // only nulls in the frame
                    output.appendNull();
                    return;
                }
                // use the recorded non-null position
                windowIndex.appendTo(argumentChannel, recordedValuePosition, output);
                return;
            }
            // try to use the recorded null position
            if (recordedNullPosition >= 0 && frameStart <= recordedNullPosition) {
                if (frameEnd <= recordedNullPosition) {
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
        if (recordedFrameStart >= 0 && recordedNullPosition >= 0 && frameStart >= recordedFrameStart && frameStart <= recordedNullPosition) {
            valuePosition = recordedNullPosition;
        }
        else {
            valuePosition = frameStart;
        }

        recordedFrameStart = frameStart;
        recordedValuePosition = -1;
        recordedNullPosition = -1;

        while (valuePosition >= 0 && valuePosition <= frameEnd) {
            if (!windowIndex.isNull(argumentChannel, valuePosition)) {
                break;
            }

            valuePosition++;
        }

        if (valuePosition > frameEnd) {
            recordedNullPosition = frameEnd;
            output.appendNull();
            return;
        }

        recordedValuePosition = valuePosition;
        windowIndex.appendTo(argumentChannel, valuePosition, output);
    }
}
