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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

/**
 * NULLIF(V1,V2): CASE WHEN V1=V2 THEN NULL ELSE V1 END
 */
@Immutable
public class NullIfExpression
        extends Expression
{
    private final Expression first;
    private final Expression second;

    @JsonCreator
    public NullIfExpression(
            @JsonProperty("first") Expression first,
            @JsonProperty("second") Expression second)
    {
        this.first = first;
        this.second = second;
    }

    @JsonProperty
    public Expression getFirst()
    {
        return first;
    }

    @JsonProperty
    public Expression getSecond()
    {
        return second;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitNullIfExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(first, second);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        NullIfExpression that = (NullIfExpression) o;
        return Objects.equals(first, that.first) &&
                Objects.equals(second, that.second);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(first, second);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
