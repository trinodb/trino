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
package io.trino.sql.planner.optimizations;

import com.google.common.collect.Range;

import static java.util.Objects.requireNonNull;

public record Cardinality(Range<Long> cardinalityRange)
{
    public Cardinality
    {
        requireNonNull(cardinalityRange, "cardinalityRange is null");
    }

    public boolean isEmpty()
    {
        return isAtMost(0);
    }

    public boolean isScalar()
    {
        return Range.singleton(1L).encloses(cardinalityRange);
    }

    public boolean isAtLeastScalar()
    {
        return isAtLeast(1L);
    }

    public boolean isAtMostScalar()
    {
        return isAtMost(1L);
    }

    public boolean isAtLeast(long minCardinality)
    {
        return Range.atLeast(minCardinality).encloses(cardinalityRange);
    }

    public boolean isAtMost(long maxCardinality)
    {
        return Range.closed(0L, maxCardinality).encloses(cardinalityRange);
    }
}
