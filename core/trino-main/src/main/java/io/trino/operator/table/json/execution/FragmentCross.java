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

public class FragmentCross
        implements JsonTableProcessingFragment
{
    private final List<JsonTableProcessingFragment> siblings;
    private final int[] outputLayout;

    private Page input;
    private int position;
    private JsonNode currentItem;
    private int currentSiblingIndex;

    public FragmentCross(List<JsonTableProcessingFragment> siblings)
    {
        this.siblings = ImmutableList.copyOf(siblings);
        checkArgument(siblings.size() >= 2, "less than 2 siblings in Cross node");
        this.outputLayout = siblings.stream()
                .map(JsonTableProcessingFragment::getOutputLayout)
                .flatMapToInt(Arrays::stream)
                .toArray();
    }

    @Override
    public void reset(JsonNode item, Page input, int position)
    {
        this.currentItem = requireNonNull(item, "item is null");
        this.input = requireNonNull(input, "input is null");
        this.position = position;
        siblings.get(0).reset(item, input, position);
        this.currentSiblingIndex = 0;
    }

    /**
     * All values produced by the siblings are stored on corresponding positions in `newRow`. It is a temporary representation of the result row, and is shared by all Fragments.
     * The values in `newRow` are not cleared between subsequent calls to getRow(), so that the parts which do not change are automatically reused.
     */
    @Override
    public boolean getRow()
    {
        while (currentSiblingIndex >= 0) {
            boolean currentSiblingProducedRow = siblings.get(currentSiblingIndex).getRow();
            if (currentSiblingProducedRow) {
                for (int i = currentSiblingIndex + 1; i < siblings.size(); i++) {
                    JsonTableProcessingFragment sibling = siblings.get(i);
                    sibling.reset(currentItem, input, position);
                    boolean siblingProducedRow = sibling.getRow();
                    if (!siblingProducedRow) {
                        // if any sibling is empty, the whole CROSS fragment is empty
                        return false;
                    }
                }
                currentSiblingIndex = siblings.size() - 1;
                return true;
            }

            // current sibling is finished
            currentSiblingIndex--;
        }

        // fragment is finished
        return false;
    }

    @Override
    public int[] getOutputLayout()
    {
        return outputLayout;
    }
}
