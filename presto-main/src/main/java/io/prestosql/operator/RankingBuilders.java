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
package io.prestosql.operator;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.GroupedTopNBuilder.RankingFunction;

import java.util.Comparator;

public final class RankingBuilders
{
    private RankingBuilders() {}

    public static RankingBuilder createRankingBuilder(
            RankingFunction rankingFunction,
            Comparator<Row> comparator,
            ObjectBigArray<PageReference> pageReferences,
            int topN)
    {
        switch (rankingFunction) {
            case ROW_NUMBER:
                return new RowNumberBuilder(comparator, pageReferences, topN);
            case RANK:
                return new RankBuilder(comparator, pageReferences, topN);
        }
        throw new IllegalArgumentException("Unsupported rankingFunction: " + rankingFunction);
    }
}
