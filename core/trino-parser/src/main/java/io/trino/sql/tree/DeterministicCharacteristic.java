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

public final class DeterministicCharacteristic
        extends RoutineCharacteristic
{
    private final boolean deterministic;

    public DeterministicCharacteristic(NodeLocation location, boolean deterministic)
    {
        super(Optional.of(location));
        this.deterministic = deterministic;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitDeterministicCharacteristic(this, context);
    }

    @Override
    public List<? extends Node> getChildren()
    {
        return ImmutableList.of();
    }

    @Override
    public boolean equals(Object obj)
    {
        return (obj instanceof DeterministicCharacteristic other) &&
                (deterministic == other.deterministic);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(deterministic);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("deterministic", deterministic)
                .toString();
    }
}
