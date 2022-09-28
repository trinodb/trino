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
package io.trino.operator.project;

import io.trino.operator.CompletedWork;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.Work;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.Type;

import static java.util.Objects.requireNonNull;

public class InputPageProjection
        implements PageProjection
{
    private final Type type;
    private final InputChannels inputChannels;

    public InputPageProjection(int inputChannel, Type type)
    {
        this.type = type;
        this.inputChannels = new InputChannels(inputChannel);
    }

    @Override
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        Block block = page.getBlock(0);
        requireNonNull(selectedPositions, "selectedPositions is null");

        // TODO: make it lazy when MergePages have better merging heuristics for small lazy pages
        if (selectedPositions.isList()) {
            block = block.copyPositions(selectedPositions.getPositions(), selectedPositions.getOffset(), selectedPositions.size());
        }
        else if (selectedPositions.size() == block.getPositionCount()) {
            return new CompletedWork<>(block);
        }
        else {
            block = block.getRegion(selectedPositions.getOffset(), selectedPositions.size());
        }
        return new CompletedWork<>(block);
    }
}
