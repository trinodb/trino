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

import static java.util.Objects.requireNonNull;

public class StaticMethodCall
        extends Expression
{
    private final QualifiedName type;
    private final Identifier method;
    private final List<Expression> arguments;

    public StaticMethodCall(NodeLocation location, QualifiedName type, Identifier method, List<Expression> arguments)
    {
        super(location);
        this.type = requireNonNull(type, "type is null");
        this.method = requireNonNull(method, "method is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public QualifiedName getType()
    {
        return type;
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
                .addAll(arguments)
                .build();
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
        StaticMethodCall other = (StaticMethodCall) obj;
        return Objects.equals(type, other.type) &&
                Objects.equals(method, other.method) &&
                Objects.equals(arguments, other.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, method, arguments);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }
        StaticMethodCall otherInvocation = (StaticMethodCall) other;
        return type.equals(otherInvocation.type) &&
                method.equals(otherInvocation.method);
    }
}
