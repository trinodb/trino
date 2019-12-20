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
package io.prestosql.sql.tree;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Format
        extends Expression
{
    private final List<Expression> arguments;

    public Format(List<Expression> arguments)
    {
        this(Optional.empty(), arguments);
    }

    public Format(NodeLocation location, List<Expression> arguments)
    {
        this(Optional.of(location), arguments);
    }

    private Format(Optional<NodeLocation> location, List<Expression> arguments)
    {
        super(location);
        requireNonNull(arguments, "arguments is null");
        checkArgument(arguments.size() >= 2, "must have at least two arguments");
        this.arguments = ImmutableList.copyOf(arguments);
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitFormat(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return arguments;
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

        Format o = (Format) obj;
        return Objects.equals(arguments, o.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(arguments);
    }
}
