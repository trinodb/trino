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
package io.trino.plugin.mongodb;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class MongoTable
{
    private final MongoTableHandle tableHandle;
    private final List<MongoColumnHandle> columns;
    private final List<MongoIndex> indexes;
    private final Optional<String> comment;

    public MongoTable(MongoTableHandle tableHandle, List<MongoColumnHandle> columns, List<MongoIndex> indexes, Optional<String> comment)
    {
        this.tableHandle = tableHandle;
        this.columns = ImmutableList.copyOf(columns);
        this.indexes = ImmutableList.copyOf(indexes);
        this.comment = requireNonNull(comment, "comment is null");
    }

    public MongoTableHandle getTableHandle()
    {
        return tableHandle;
    }

    public List<MongoColumnHandle> getColumns()
    {
        return columns;
    }

    public List<MongoIndex> getIndexes()
    {
        return indexes;
    }

    public Optional<String> getComment()
    {
        return comment;
    }

    @Override
    public int hashCode()
    {
        return tableHandle.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof MongoTable)) {
            return false;
        }
        MongoTable that = (MongoTable) obj;
        return this.tableHandle.equals(that.tableHandle);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("tableHandle", tableHandle)
                .toString();
    }
}
