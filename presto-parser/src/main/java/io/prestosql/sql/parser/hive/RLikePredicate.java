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
package io.prestosql.sql.parser.hive;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.tree.*;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class RLikePredicate
        extends LikePredicate
{
    private final Expression value;
    private final Expression pattern;
    private final Optional<Expression> escape;

    public RLikePredicate(Expression value, Expression pattern, Expression escape)
    {
        this(Optional.empty(), value, pattern, Optional.of(escape));
    }

    public RLikePredicate(NodeLocation location, Expression value, Expression pattern, Optional<Expression> escape)
    {
        this(Optional.of(location), value, pattern, escape);
    }

    public RLikePredicate(Expression value, Expression pattern, Optional<Expression> escape)
    {
        this(Optional.empty(), value, pattern, escape);
    }

    private RLikePredicate(Optional<NodeLocation> location, Expression value, Expression pattern, Optional<Expression> escape)
    {
        super(location, value, pattern, escape);
        requireNonNull(value, "value is null");
        requireNonNull(pattern, "pattern is null");
        requireNonNull(escape, "escape is null");

        this.value = value;
        this.pattern = pattern;
        this.escape = escape;
    }

    public Expression getValue()
    {
        return value;
    }

    public Expression getPattern()
    {
        return pattern;
    }

    public Optional<Expression> getEscape()
    {
        return escape;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitRLikePredicate(this, context);
    }
}
