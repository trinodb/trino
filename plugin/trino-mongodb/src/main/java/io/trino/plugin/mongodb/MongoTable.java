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

import static java.util.Objects.requireNonNull;

public record MongoTable(MongoTableHandle tableHandle, List<MongoColumnHandle> columns, List<MongoIndex> indexes, Optional<String> comment)
{
    public MongoTable
    {
        requireNonNull(tableHandle, "tableHandle is null");
        columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
        indexes = ImmutableList.copyOf(requireNonNull(indexes, "indexes is null"));
        requireNonNull(comment, "comment is null");
    }
}
