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

import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.plan.WindowNode;

import java.util.Optional;

class WindowFrameMatcher
{
    private WindowFrameMatcher() {}

    public static boolean matches(WindowNode.Frame expected, WindowNode.Frame actual, SymbolAliases aliases)
    {
        return expected.getType().equals(actual.getType())
                && expected.getStartType().equals(actual.getStartType())
                && expected.getEndType().equals(actual.getEndType())
                && matches(expected.getStartValue(), actual.getStartValue(), aliases)
                && matches(expected.getSortKeyCoercedForFrameEndComparison(), actual.getSortKeyCoercedForFrameEndComparison(), aliases)
                && matches(expected.getEndValue(), actual.getEndValue(), aliases)
                && matches(expected.getSortKeyCoercedForFrameEndComparison(), actual.getSortKeyCoercedForFrameEndComparison(), aliases);
    }

    private static boolean matches(Optional<Symbol> expected, Optional<Symbol> actual, SymbolAliases aliases)
    {
        if (expected.isPresent() != actual.isPresent()) {
            return false;
        }

        return expected.map(symbol -> aliases.get(symbol.name()).name().equals(actual.get().name()))
                .orElse(true);
    }
}
