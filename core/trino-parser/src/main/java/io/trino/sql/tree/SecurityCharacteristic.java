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
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class SecurityCharacteristic
        extends RoutineCharacteristic
{
    public enum Security
    {
        INVOKER, DEFINER
    }

    private final Security security;

    public SecurityCharacteristic(Security security)
    {
        this(Optional.empty(), security);
    }

    public SecurityCharacteristic(NodeLocation location, Security security)
    {
        this(Optional.of(location), security);
    }

    private SecurityCharacteristic(Optional<NodeLocation> location, Security security)
    {
        super(location);
        this.security = requireNonNull(security, "security is null");
    }

    public Security getSecurity()
    {
        return security;
    }

    @Override
    protected <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSecurityCharacteristic(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof SecurityCharacteristic other) &&
                (security == other.security);
    }

    @Override
    public int hashCode()
    {
        return security.hashCode();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("security", security)
                .toString();
    }
}
