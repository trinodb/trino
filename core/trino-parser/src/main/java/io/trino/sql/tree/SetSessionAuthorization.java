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

public class SetSessionAuthorization
        extends Statement
{
    private final Expression user;

    public SetSessionAuthorization(Expression user)
    {
        this(Optional.empty(), user);
    }

    public SetSessionAuthorization(NodeLocation location, Expression user)
    {
        this(Optional.of(location), user);
    }

    private SetSessionAuthorization(Optional<NodeLocation> location, Expression user)
    {
        super(location);
        this.user = requireNonNull(user, "user is null");
    }

    public Expression getUser()
    {
        return user;
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSetSessionAuthorization(this, context);
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
        SetSessionAuthorization setSessionAuthorization = (SetSessionAuthorization) o;
        return Objects.equals(user, setSessionAuthorization.user);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(user);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("user", user)
                .toString();
    }
}
