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
import io.trino.spi.Page;

public interface JsonTableProcessingFragment
{
    /**
     * Prepares the Fragment to produce rows for the new JSON item.
     * Note: This method must be called for each new JSON item. Due to nesting, there might be multiple JSON items to process for a single position in the input page.
     * Therefore, input and position may not change for subsequent calls.
     *
     * @param item the new JSON item
     * @param input the input Page currently processed by json_table function
     * @param position the currently processed position in the input page
     */
    void reset(JsonNode item, Page input, int position);

    /**
     * Prepares the root Fragment to produce rows for the new JSON item and new set of path parameters.
     * Note: at the root level, there is one JSON item and one set of path parameters to process for each position in the input page.
     *
     * @param item the new JSON item
     * @param input the input Page currently processed by json_table function
     * @param position the currently processed position in the input page
     * @param pathParameters JSON path parameters for the top-level JSON path
     */
    default void resetRoot(JsonNode item, Page input, int position, Object[] pathParameters)
    {
        throw new IllegalStateException("not the root fragment");
    }

    /**
     * Tries to produce output values for all columns included in the Fragment,
     * and stores them in corresponding positions in `newRow`.
     * Note: According to OUTER or UNION semantics, some values might be null-padded instead of computed.
     * Note: a single JSON item might result in multiple output rows. To fully process a JSON item, the caller must:
     * - reset the Fragment with the JSON item
     * - call getRow() and collect output rows as long as `true` is returned
     * If `false` is returned, there is no output row available, and the JSON item is fully processed
     *
     * @return true if row was produced, false if row was not produced (Fragment is finished)
     */
    boolean getRow();

    /**
     * Returns an array containing indexes of columns produced by the fragment within all columns produced by json_table.
     */
    int[] getOutputLayout();
}
