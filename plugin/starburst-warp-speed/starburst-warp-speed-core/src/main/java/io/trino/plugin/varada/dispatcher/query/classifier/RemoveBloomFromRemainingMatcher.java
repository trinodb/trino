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
package io.trino.plugin.varada.dispatcher.query.classifier;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.varada.dispatcher.model.VaradaColumn;
import io.trino.plugin.varada.dispatcher.query.PredicateContext;
import io.trino.plugin.varada.dispatcher.query.data.QueryColumn;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

// TODO: On mix query, we ignore remainingTupleDomain anyway. Consider cleaning it completely instead of just removing bloom.
class RemoveBloomFromRemainingMatcher
        implements Matcher
{
    public RemoveBloomFromRemainingMatcher()
    {
    }

    @Override
    public MatchContext match(ClassifyArgs classifyArgs, MatchContext matchContext)
    {
        Set<VaradaColumn> bloomColumns = matchContext.matchDataList().stream()
                .filter(queryMatchData -> queryMatchData.getWarmUpElement().getWarmUpType().bloom())
                .map(QueryColumn::getVaradaColumn)
                .collect(Collectors.toSet());
        ImmutableMap.Builder<VaradaColumn, PredicateContext> remainingPredicateContextWithoutBloom = ImmutableMap.builder();
        for (Map.Entry<VaradaColumn, PredicateContext> predicateColumn : matchContext.remainingPredicateContext().entrySet()) {
            if (!bloomColumns.contains(predicateColumn.getKey())) {
                remainingPredicateContextWithoutBloom.put(predicateColumn.getKey(), predicateColumn.getValue());
            }
        }
        return new MatchContext(matchContext.matchDataList(), remainingPredicateContextWithoutBloom.buildOrThrow(), true);
    }
}
