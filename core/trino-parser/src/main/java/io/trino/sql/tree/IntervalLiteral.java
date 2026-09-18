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

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IntervalLiteral
        extends Literal
{
    public enum Sign
    {
        POSITIVE {
            @Override
            public int multiplier()
            {
                return 1;
            }
        },
        NEGATIVE {
            @Override
            public int multiplier()
            {
                return -1;
            }
        };

        public abstract int multiplier();
    }

    private final String value;
    private final Sign sign;
    private final IntervalQualifier qualifier;

    @Deprecated
    public IntervalLiteral(String value, Sign sign, IntervalQualifier qualifier)
    {
        this(Optional.empty(), value, sign, qualifier);
    }

    public IntervalLiteral(NodeLocation location, String value, Sign sign, IntervalQualifier qualifier)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
        this.sign = requireNonNull(sign, "sign is null");
        this.qualifier = requireNonNull(qualifier, "qualifier is null");
    }

    private IntervalLiteral(Optional<NodeLocation> location, String value, Sign sign, IntervalQualifier qualifier)
    {
        super(location);
        requireNonNull(value, "value is null");
        requireNonNull(sign, "sign is null");
        requireNonNull(qualifier, "qualifier is null");

        this.value = value;
        this.sign = sign;
        this.qualifier = qualifier;
    }

    public String getValue()
    {
        return value;
    }

    public Sign getSign()
    {
        return sign;
    }

    public IntervalQualifier qualifier()
    {
        return qualifier;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIntervalLiteral(this, context);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof IntervalLiteral that)) {
            return false;
        }
        return Objects.equals(value, that.value) &&
                sign == that.sign &&
                Objects.equals(qualifier, that.qualifier);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(value, sign, qualifier);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        IntervalLiteral otherLiteral = (IntervalLiteral) other;
        return Objects.equals(this.value, otherLiteral.value) &&
                this.sign == otherLiteral.sign &&
                Objects.equals(this.qualifier, otherLiteral.qualifier);
    }
}
