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
package io.trino.operator.table.json.execution;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class FragmentUnion
        implements JsonTableProcessingFragment
{
    private final List<JsonTableProcessingFragment> siblings;
    private final int[] outputLayout;

    // the place where the computed values (or nulls) are stored while computing an output row
    private final Object[] newRow;

    private int currentSiblingIndex;

    public FragmentUnion(List<JsonTableProcessingFragment> siblings, Object[] newRow)
    {
        this.siblings = ImmutableList.copyOf(siblings);
        checkArgument(siblings.size() >= 2, "less than 2 siblings in Union node");
        this.outputLayout = siblings.stream()
                .map(JsonTableProcessingFragment::getOutputLayout)
                .flatMapToInt(Arrays::stream)
                .toArray();
        this.newRow = requireNonNull(newRow, "newRow is null");
    }

    @Override
    public void reset(JsonNode item, Page input, int position)
    {
        requireNonNull(item, "item is null");
        requireNonNull(input, "input is null");
        siblings.stream()
                .forEach(sibling -> sibling.reset(item, input, position));
        this.currentSiblingIndex = 0;
        appendNulls(this);
    }

    /**
     * The values produced by the current sibling are stored on corresponding positions in `newRow`, and for other siblings `newRow` is filled with nulls.
     * The values in `newRow` are not cleared between subsequent calls to getRow(), so that the parts which do not change are automatically reused.
     */
    @Override
    public boolean getRow()
    {
        while (true) {
            if (currentSiblingIndex >= siblings.size()) {
                // fragment is finished
                return false;
            }

            JsonTableProcessingFragment currentSibling = siblings.get(currentSiblingIndex);
            boolean currentSiblingProducedRow = currentSibling.getRow();
            if (currentSiblingProducedRow) {
                return true;
            }

            // current sibling is finished
            appendNulls(currentSibling);
            currentSiblingIndex++;
        }
    }

    private void appendNulls(JsonTableProcessingFragment fragment)
    {
        for (int column : fragment.getOutputLayout()) {
            newRow[column] = null;
        }
    }

    @Override
    public int[] getOutputLayout()
    {
        return outputLayout;
    }
}
