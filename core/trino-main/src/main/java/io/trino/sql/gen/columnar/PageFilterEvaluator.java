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

import io.trino.operator.project.DictionaryAwarePageFilter;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

public final class PageFilterEvaluator
        implements FilterEvaluator
{
    private final PageFilter filter;

    public PageFilterEvaluator(PageFilter filter)
    {
        if (filter.getInputChannels().size() == 1 && filter.isDeterministic()) {
            this.filter = new DictionaryAwarePageFilter(filter);
        }
        else {
            this.filter = filter;
        }
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        SourcePage inputPage = filter.getInputChannels().getInputChannels(page);
        long start = System.nanoTime();
        SelectedPositions selectedPositions = filter.filter(session, inputPage);
        return new SelectionResult(selectedPositions, System.nanoTime() - start);
    }
}
