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
package io.trino.operator.window;

import io.trino.sql.tree.FrameBound;
import io.trino.sql.tree.SortItem.Ordering;
import io.trino.sql.tree.WindowFrame;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class FrameInfo
{
    private final WindowFrame.Type type;
    private final FrameBound.Type startType;
    private final int startChannel;
    private final int sortKeyChannelForStartComparison;
    private final FrameBound.Type endType;
    private final int endChannel;
    private final int sortKeyChannelForEndComparison;
    private final int sortKeyChannel;
    private final Optional<Ordering> ordering;

    public FrameInfo(
            WindowFrame.Type type,
            FrameBound.Type startType,
            Optional<Integer> startChannel,
            Optional<Integer> sortKeyChannelForStartComparison,
            FrameBound.Type endType,
            Optional<Integer> endChannel,
            Optional<Integer> sortKeyChannelForEndComparison,
            Optional<Integer> sortKeyChannel,
            Optional<Ordering> ordering)
    {
        this.type = requireNonNull(type, "type is null");
        this.startType = requireNonNull(startType, "startType is null");
        this.startChannel = startChannel.orElse(-1);
        this.sortKeyChannelForStartComparison = sortKeyChannelForStartComparison.orElse(-1);
        this.endType = requireNonNull(endType, "endType is null");
        this.endChannel = endChannel.orElse(-1);
        this.sortKeyChannelForEndComparison = sortKeyChannelForEndComparison.orElse(-1);
        this.sortKeyChannel = sortKeyChannel.orElse(-1);
        this.ordering = requireNonNull(ordering, "ordering is null");
    }

    public WindowFrame.Type getType()
    {
        return type;
    }

    public FrameBound.Type getStartType()
    {
        return startType;
    }

    public int getStartChannel()
    {
        return startChannel;
    }

    public int getSortKeyChannelForStartComparison()
    {
        return sortKeyChannelForStartComparison;
    }

    public FrameBound.Type getEndType()
    {
        return endType;
    }

    public int getEndChannel()
    {
        return endChannel;
    }

    public int getSortKeyChannelForEndComparison()
    {
        return sortKeyChannelForEndComparison;
    }

    public int getSortKeyChannel()
    {
        return sortKeyChannel;
    }

    public Optional<Ordering> getOrdering()
    {
        return ordering;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, startType, startChannel, sortKeyChannelForStartComparison, endType, endChannel, sortKeyChannelForEndComparison, sortKeyChannel, ordering);
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

        FrameInfo other = (FrameInfo) obj;

        return this.type == other.type &&
                this.startType == other.startType &&
                Objects.equals(this.startChannel, other.startChannel) &&
                Objects.equals(this.sortKeyChannelForStartComparison, other.sortKeyChannelForStartComparison) &&
                this.endType == other.endType &&
                Objects.equals(this.endChannel, other.endChannel) &&
                Objects.equals(this.sortKeyChannelForEndComparison, other.sortKeyChannelForEndComparison) &&
                Objects.equals(this.sortKeyChannel, other.sortKeyChannel) &&
                Objects.equals(this.ordering, other.ordering);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", type)
                .add("startType", startType)
                .add("startChannel", startChannel)
                .add("sortKeyChannelForStartComparison", sortKeyChannelForStartComparison)
                .add("endType", endType)
                .add("endChannel", endChannel)
                .add("sortKeyChannelForEndComparison", sortKeyChannelForEndComparison)
                .add("sortKeyChannel", sortKeyChannel)
                .add("ordering", ordering)
                .toString();
    }
}
