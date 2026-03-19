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
package io.trino.sql.planner.assertions;

import io.trino.sql.ir.Reference;

import static java.util.Objects.requireNonNull;

public class MatchResult
{
    public static final MatchResult NO_MATCH = new MatchResult(false, new SymbolAliases(), new MatchingDynamicFilters());

    private final boolean matches;
    private final SymbolAliases newAliases;
    private final MatchingDynamicFilters dynamicFilters;

    public static MatchResult match()
    {
        return new MatchResult(true, new SymbolAliases(), new MatchingDynamicFilters());
    }

    public static MatchResult match(String alias, Reference reference)
    {
        SymbolAliases newAliases = SymbolAliases.builder()
                .put(alias, reference)
                .build();
        return new MatchResult(true, newAliases, new MatchingDynamicFilters());
    }

    public static MatchResult match(SymbolAliases newAliases)
    {
        return new MatchResult(true, newAliases, new MatchingDynamicFilters());
    }

    public static MatchResult match(SymbolAliases newAliases, MatchingDynamicFilters dynamicFilters)
    {
        return new MatchResult(true, newAliases, dynamicFilters);
    }

    public static MatchResult match(MatchingDynamicFilters dynamicFilters)
    {
        return new MatchResult(true, new SymbolAliases(), dynamicFilters);
    }

    public MatchResult(boolean matches)
    {
        this(matches, new SymbolAliases(), new MatchingDynamicFilters());
    }

    private MatchResult(boolean matches, SymbolAliases newAliases, MatchingDynamicFilters dynamicFilters)
    {
        this.matches = matches;
        this.newAliases = requireNonNull(newAliases, "newAliases is null");
        this.dynamicFilters = requireNonNull(dynamicFilters, "dynamicFilters is null");
    }

    public boolean isMatch()
    {
        return matches;
    }

    public SymbolAliases getAliases()
    {
        return newAliases;
    }

    public MatchingDynamicFilters getDynamicFilters()
    {
        return dynamicFilters;
    }

    @Override
    public String toString()
    {
        if (matches) {
            return "MATCH";
        }
        return "NO MATCH";
    }
}
