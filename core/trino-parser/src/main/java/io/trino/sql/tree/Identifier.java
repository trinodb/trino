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

import com.google.common.base.CharMatcher;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class Identifier
        extends Expression
{
    private static final CharMatcher FIRST_CHAR_DISALLOWED_MATCHER = CharMatcher.inRange('0', '9')
            .precomputed();

    private static final CharMatcher ALLOWED_CHARS_MATCHER = CharMatcher.inRange('a', 'z')
            .or(CharMatcher.inRange('A', 'Z'))
            .or(CharMatcher.is('_'))
            .or(CharMatcher.inRange('0', '9'))
            .precomputed();

    private final String value;
    private final boolean delimited;

    public Identifier(NodeLocation location, String value, boolean delimited)
    {
        this(Optional.of(location), value, delimited);
    }

    public Identifier(String value, boolean delimited)
    {
        this(Optional.empty(), value, delimited);
    }

    public Identifier(String value)
    {
        this(Optional.empty(), value, !isValidIdentifier(value));
    }

    private Identifier(Optional<NodeLocation> location, String value, boolean delimited)
    {
        super(location);
        this.value = requireNonNull(value, "value is null");
        this.delimited = delimited;

        checkArgument(!value.isEmpty(), "value is empty");
        checkArgument(delimited || isValidIdentifier(value), "value contains illegal characters: %s", value);
    }

    public String getValue()
    {
        return value;
    }

    public boolean isDelimited()
    {
        return delimited;
    }

    public String getCanonicalValue()
    {
        if (isDelimited()) {
            return value;
        }

        return value.toUpperCase(ENGLISH);
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitIdentifier(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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

        Identifier that = (Identifier) o;
        return Objects.equals(value, that.value);
    }

    @Override
    public int hashCode()
    {
        return value.hashCode();
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        Identifier that = (Identifier) other;
        return Objects.equals(value, that.value) && delimited == that.delimited;
    }

    private static boolean isValidIdentifier(String value)
    {
        verify(!Strings.isNullOrEmpty(value), "Identifier cannot be empty or null");

        if (FIRST_CHAR_DISALLOWED_MATCHER.matches(value.charAt(0))) {
            return false;
        }

        // We've already checked that first char does not contain digits,
        // so to avoid copying we are checking whole string.
        return ALLOWED_CHARS_MATCHER.matchesAllOf(value);
    }
}
