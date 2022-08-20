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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.sql.tree.SortItem.NullOrdering;
import io.trino.sql.tree.SortItem.Ordering;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

@Immutable
public class SortItem
        extends Node
{
    private final Expression sortKey;
    private final Ordering ordering;
    private final NullOrdering nullOrdering;

    @JsonCreator
    public SortItem(
            @JsonProperty("sortKey") Expression sortKey,
            @JsonProperty("ordering") Ordering ordering,
            @JsonProperty("nullOrdering") NullOrdering nullOrdering)
    {
        this.ordering = ordering;
        this.sortKey = sortKey;
        this.nullOrdering = nullOrdering;
    }

    @JsonProperty
    public Expression getSortKey()
    {
        return sortKey;
    }

    @JsonProperty
    public Ordering getOrdering()
    {
        return ordering;
    }

    @JsonProperty
    public NullOrdering getNullOrdering()
    {
        return nullOrdering;
    }

    @Override
    public <R, C> R accept(IrVisitor<R, C> visitor, C context)
    {
        return visitor.visitSortItem(this, context);
    }

    @Override
    public List<Node> getChildren()
    {
        return ImmutableList.of(sortKey);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sortKey", sortKey)
                .add("ordering", ordering)
                .add("nullOrdering", nullOrdering)
                .toString();
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

        SortItem sortItem = (SortItem) o;
        return Objects.equals(sortKey, sortItem.sortKey) &&
                (ordering == sortItem.ordering) &&
                (nullOrdering == sortItem.nullOrdering);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(sortKey, ordering, nullOrdering);
    }

    @Override
    public boolean shallowEquals(Node other)
    {
        if (!sameClass(this, other)) {
            return false;
        }

        SortItem otherItem = (SortItem) other;
        return ordering == otherItem.ordering && nullOrdering == otherItem.nullOrdering;
    }
}
