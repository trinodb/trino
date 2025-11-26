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
package io.trino.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

public class StaticMethodCall
        extends Expression
{
    private final Expression target;
    private final Identifier method;
    private final List<Expression> arguments;

    public StaticMethodCall(NodeLocation location, Expression target, Identifier method, List<Expression> arguments)
    {
        super(location);
        this.target = target;
        this.method = method;
        this.arguments = arguments;
    }

    public Expression getTarget()
    {
        return target;
    }

    public Identifier getMethod()
    {
        return method;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitStaticMethodCall(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(target)
                .add(method)
                .addAll(arguments)
                .build();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        return true;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof StaticMethodCall that)) {
            return false;
        }
        return Objects.equals(target, that.target) && Objects.equals(method, that.method) && Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(target, method, arguments);
    }
}
