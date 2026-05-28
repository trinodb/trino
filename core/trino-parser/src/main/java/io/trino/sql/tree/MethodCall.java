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

public class MethodCall
        extends Expression
{
    private final Expression receiver;
    private final Identifier method;
    private final List<Expression> arguments;

    public MethodCall(NodeLocation location, Expression receiver, Identifier method, List<Expression> arguments)
    {
        super(location);
        this.receiver = requireNonNull(receiver, "receiver is null");
        this.method = requireNonNull(method, "method is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    public Expression getReceiver()
    {
        return receiver;
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
        return visitor.visitMethodCall(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.<Node>builder()
                .add(receiver)
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
        MethodCall other = (MethodCall) obj;
        return Objects.equals(receiver, other.receiver) &&
                Objects.equals(method, other.method) &&
                Objects.equals(arguments, other.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(receiver, method, arguments);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }
        MethodCall otherInvocation = (MethodCall) other;
        return method.equals(otherInvocation.method);
    }
}
