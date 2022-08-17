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

import static java.util.Objects.requireNonNull;

@Immutable
public class LambdaExpression
        extends Expression
{
    private final List<LambdaArgumentDeclaration> arguments;
    private final Expression body;

    @JsonCreator
    public LambdaExpression(
            @JsonProperty("arguments") List<LambdaArgumentDeclaration> arguments,
            @JsonProperty("body") Expression body)
    {
        this.arguments = requireNonNull(arguments, "arguments is null");
        this.body = requireNonNull(body, "body is null");
    }

    @JsonProperty
    public List<LambdaArgumentDeclaration> getArguments()
    {
        return arguments;
    }

    @JsonProperty
    public Expression getBody()
    {
        return body;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitLambdaExpression(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        ImmutableList.Builder<Node> nodes = ImmutableList.builder();
        nodes.addAll(arguments);
        nodes.add(body);
        return nodes.build();
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
        LambdaExpression that = (LambdaExpression) obj;
        return Objects.equals(arguments, that.arguments) &&
                Objects.equals(body, that.body);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments, body);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        return sameClass(this, other);
    }
}
