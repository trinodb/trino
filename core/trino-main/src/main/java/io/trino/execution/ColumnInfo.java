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
package io.trino.execution;

import java.util.Optional;

// XXX: ColumnInfo allows the grouping of all the naming elements necessary to deliver complete ResultSetMetaData (see issue#22306).
public record ColumnInfo(Optional<String> catalog, Optional<String> schema, Optional<String> table, String name, Optional<String> label)
{
    // XXX: This constructor is used by the PlanBuilder.output() method and allows you to create ColumnInfo using only a column name.
    public ColumnInfo(String name)
    {
        this(Optional.empty(), Optional.empty(), Optional.empty(), name, Optional.of(name));
    }
}
