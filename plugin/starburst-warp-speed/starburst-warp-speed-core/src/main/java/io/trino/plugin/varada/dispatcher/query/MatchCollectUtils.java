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
package io.trino.plugin.varada.dispatcher.query;

import io.trino.plugin.varada.dispatcher.query.data.collect.NativeQueryCollectData;
import io.trino.plugin.varada.dispatcher.query.data.match.QueryMatchData;

import java.util.List;
import java.util.Optional;

public class MatchCollectUtils
{
    private MatchCollectUtils() {}

    public static Optional<QueryMatchData> findMatchForMatchCollect(NativeQueryCollectData collect, List<QueryMatchData> matches)
    {
        return matches.stream()
                .filter(queryMatchData -> canMatchForMatchCollect(queryMatchData, collect))
                .findFirst();
    }

    public static boolean canMatchForMatchCollect(QueryMatchData match, NativeQueryCollectData collect)
    {
        return !match.isPartOfLogicalOr() && match.canMatchCollect(collect.getVaradaColumn());
    }

    // Given a collect that is a match-collect, return if it can still be a match-collect using one of the supplied matches
    public static boolean canStillBeMatchCollected(NativeQueryCollectData collect, List<QueryMatchData> matches)
    {
        return collect.getMatchCollectType() != MatchCollectType.DISABLED &&
                findMatchForMatchCollect(collect, matches).isPresent();
    }

    public static boolean canBeMatchForMatchCollect(QueryMatchData match, List<NativeQueryCollectData> collects)
    {
        return collects.stream()
                .anyMatch(collect -> collect.getMatchCollectType() != MatchCollectType.DISABLED &&
                        canMatchForMatchCollect(match, collect));
    }

    public enum MatchCollectType
    {
        DISABLED,
        ORDINARY,
        MAPPED
    }
}
