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
package io.trino.sql.gen.columnar;

import io.trino.operator.project.InputChannels;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

/**
 * Implementations of this interface evaluate a filter on the input Page.
 * <p>
 * {@link FilterEvaluator} will call one of filterPositionsRange or filterPositionsList depending on whether the
 * active SelectionPositions are stored in a list or in a range.
 * <p>
 * Implementations are expected to operate on a Page containing only the required channels specified by getInputChannels.
 * <p>
 * Currently, the implementations of this interface except IS_NULL and NOT(IS_NULL),
 * don't explicitly handle NULLs or indeterminate values, and just return FALSE for those cases.
 * This will need to change to allow ColumnarFilter implementations to be composed in all cases (e.g. NOT filters).
 * ColumnarFilter implementations are never composed, {@link FilterEvaluator} implementations may be composed.
 * <p>
 */
public interface ColumnarFilter
{
    /**
     * @param outputPositions list of positions active after evaluating this filter on the input loadedPage
     * @param offset start of input positions range evaluated by this filter
     * @param size length of input positions range evaluated by this filter
     * @param loadedPage input Page after using {@link ColumnarFilter#getInputChannels} to load only the required channels
     * @return count of positions active after evaluating this filter on the input loadedPage
     */
    int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage);

    /**
     * @param outputPositions list of positions active after evaluating this filter on the input loadedPage
     * @param activePositions input positions list evaluated by this filter
     * @param offset index in activePositions where the input positions evaluated by this filter start
     * @param size length after offset in activePositions where the input positions evaluated by this filter end
     * @param loadedPage input Page after using {@link ColumnarFilter#getInputChannels} to load only the required channels
     * @return count of positions active after evaluating this filter on the input loadedPage
     */
    int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage);

    /**
     * @return InputChannels of input Page that this filter operates on
     */
    InputChannels getInputChannels();
}
