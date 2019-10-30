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
package io.prestosql.plugin.jdbc;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RangeInfo
{
    private final String expression;
    /**
     * The lower bound of the range (included)
     */
    private final Optional<Integer> lowerBound;
    /**
     * The upper bound of the range (not include the upperBond itself)
     */
    private final Optional<Integer> upperBound;

    public RangeInfo(String expression, Optional<Integer> lowerBound, Optional<Integer> upperBound)
    {
        this.expression = requireNonNull(expression, "expression is null");
        this.lowerBound = requireNonNull(lowerBound, "lowerBound is null");
        this.upperBound = requireNonNull(upperBound, "upperBound is null");
    }

    public String getExpression()
    {
        return expression;
    }

    public Optional<Integer> getLowerBound()
    {
        return lowerBound;
    }

    public Optional<Integer> getUpperBound()
    {
        return upperBound;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        RangeInfo that = (RangeInfo) obj;

        return Objects.equals(this.expression, that.expression)
                && Objects.equals(this.lowerBound, that.lowerBound)
                && Objects.equals(this.upperBound, that.upperBound);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expression, lowerBound, upperBound);
    }

    @Override
    public String toString()
    {
        return "[" + lowerBound + ", " + upperBound + ")";
    }
}
