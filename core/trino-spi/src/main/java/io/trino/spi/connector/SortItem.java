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
package io.trino.spi.connector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.StringJoiner;

import static java.util.Objects.requireNonNull;

public class SortItem
{
    private final String name;
    private final SortOrder sortOrder;

    @JsonCreator
    public SortItem(String name, SortOrder sortOrder)
    {
        this.name = requireNonNull(name, "name is null");
        this.sortOrder = requireNonNull(sortOrder, "sortOrder is null");
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public SortOrder getSortOrder()
    {
        return sortOrder;
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
        return name.equals(sortItem.name) && sortOrder == sortItem.sortOrder;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, sortOrder);
    }

    @Override
    public String toString()
    {
        return new StringJoiner(", ", SortItem.class.getSimpleName() + "[", "]")
                .add("name='" + name + "'")
                .add("sortOrder=" + sortOrder)
                .toString();
    }
}
