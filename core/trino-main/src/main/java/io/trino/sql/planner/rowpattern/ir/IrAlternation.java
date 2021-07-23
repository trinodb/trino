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

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class IrAlternation
        extends IrRowPattern
{
    private final List<IrRowPattern> patterns;

    @JsonCreator
    public IrAlternation(List<IrRowPattern> patterns)
    {
        this.patterns = requireNonNull(patterns, "patterns is null");
        checkArgument(patterns.size() >= 2, "pattern alternation must have at least 2 elements (actual: %s)", patterns.size());
    }

    @JsonProperty
    public List<IrRowPattern> getPatterns()
    {
        return patterns;
    }

    @Override
    public <R, C> R accept(IrRowPatternVisitor<R, C> visitor, C context)
    {
        return visitor.visitIrAlternation(this, context);
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
        IrAlternation o = (IrAlternation) obj;
        return Objects.equals(patterns, o.patterns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(patterns);
    }

    @Override
    public String toString()
    {
        return patterns.stream()
                .map(Object::toString)
                .collect(joining(" | ", "(", ")"));
    }
}
