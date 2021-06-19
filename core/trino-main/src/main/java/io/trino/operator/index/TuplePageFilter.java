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
package io.trino.operator.index;

import com.google.common.collect.ImmutableList;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * Filters out rows that do not match the values from the specified tuple
 */
public class TuplePageFilter
        implements PageFilter
{
    private final Page tuplePage;
    private final InputChannels inputChannels;
    private final List<BlockPositionEqual> equalOperators;
    private boolean[] selectedPositions = new boolean[0];

    public TuplePageFilter(Page tuplePage, List<BlockPositionEqual> equalOperators, List<Integer> inputChannels)
    {
        requireNonNull(tuplePage, "tuplePage is null");
        requireNonNull(equalOperators, "equalOperators is null");
        requireNonNull(inputChannels, "inputChannels is null");

        checkArgument(tuplePage.getPositionCount() == 1, "tuplePage should only have one position");
        checkArgument(tuplePage.getChannelCount() == inputChannels.size(), "tuplePage and inputChannels have different number of channels");
        checkArgument(equalOperators.size() == inputChannels.size(), "equalOperators and inputChannels have different number of channels");

        this.tuplePage = tuplePage;
        this.equalOperators = ImmutableList.copyOf(equalOperators);
        this.inputChannels = new InputChannels(inputChannels);
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
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        if (selectedPositions.length < page.getPositionCount()) {
            selectedPositions = new boolean[page.getPositionCount()];
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            selectedPositions[position] = matches(page, position);
        }

        return PageFilter.positionsArrayToSelectedPositions(selectedPositions, page.getPositionCount());
    }

    private boolean matches(Page page, int position)
    {
        for (int channel = 0; channel < inputChannels.size(); channel++) {
            BlockPositionEqual equalOperator = equalOperators.get(channel);
            Block outputBlock = page.getBlock(channel);
            Block singleTupleBlock = tuplePage.getBlock(channel);
            if (!equalOperator.equal(singleTupleBlock, 0, outputBlock, position)) {
                return false;
            }
        }
        return true;
    }
}
