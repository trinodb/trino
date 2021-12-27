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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class SetTimeZone
        extends Statement
{
    private final Optional<Expression> timeZone;

    public SetTimeZone(NodeLocation location, Optional<Expression> timeZone)
    {
        super(Optional.of(location));
        requireNonNull(timeZone, "timeZone is null");
        this.timeZone = timeZone;
    }

    public Optional<Expression> getTimeZone()
    {
        return timeZone;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetTimeZone(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return timeZone.isPresent() ? ImmutableList.of(timeZone.get()) : ImmutableList.of();
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
        SetTimeZone that = (SetTimeZone) o;
        return Objects.equals(timeZone, that.timeZone);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(timeZone);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("timeZone", timeZone.isPresent() ? timeZone : "LOCAL")
                .toString();
    }
}
