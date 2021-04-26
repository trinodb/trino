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
package io.trino.sql.planner.rowpattern.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class IrQuantifier
{
    private final int atLeast;
    private final Optional<Integer> atMost;
    private final boolean greedy;

    public static IrQuantifier zeroOrMore(boolean greedy)
    {
        return new IrQuantifier(0, Optional.empty(), greedy);
    }

    public static IrQuantifier oneOrMore(boolean greedy)
    {
        return new IrQuantifier(1, Optional.empty(), greedy);
    }

    public static IrQuantifier zeroOrOne(boolean greedy)
    {
        return new IrQuantifier(0, Optional.of(1), greedy);
    }

    public static IrQuantifier range(Optional<Integer> atLeast, Optional<Integer> atMost, boolean greedy)
    {
        return new IrQuantifier(atLeast.orElse(0), atMost, greedy);
    }

    @JsonCreator
    public IrQuantifier(int atLeast, Optional<Integer> atMost, boolean greedy)
    {
        this.atLeast = atLeast;
        this.atMost = requireNonNull(atMost, "atMost is null");
        this.greedy = greedy;
    }

    @JsonProperty
    public int getAtLeast()
    {
        return atLeast;
    }

    @JsonProperty
    public Optional<Integer> getAtMost()
    {
        return atMost;
    }

    @JsonProperty
    public boolean isGreedy()
    {
        return greedy;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        IrQuantifier o = (IrQuantifier) obj;
        return atLeast == o.atLeast &&
                Objects.equals(atMost, o.atMost) &&
                greedy == o.greedy;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(atLeast, atMost, greedy);
    }

    @Override
    public String toString()
    {
        return format("{%s, %s}", atLeast, atMost.map(Object::toString).orElse("∞"));
    }
}
