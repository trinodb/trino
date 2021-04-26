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

public final class ProcessingMode
        extends Node
{
    private final Mode mode;

    public ProcessingMode(NodeLocation location, Mode mode)
    {
        this(Optional.of(location), mode);
    }

    public ProcessingMode(Optional<NodeLocation> location, Mode mode)
    {
        super(location);
        this.mode = requireNonNull(mode, "mode is null");
    }

    public Mode getMode()
    {
        return mode;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitProcessingMode(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of();
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
        return mode == ((ProcessingMode) obj).mode;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(mode);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("mode", mode)
                .toString();
    }

    public enum Mode
    {
        RUNNING, FINAL
    }
}
