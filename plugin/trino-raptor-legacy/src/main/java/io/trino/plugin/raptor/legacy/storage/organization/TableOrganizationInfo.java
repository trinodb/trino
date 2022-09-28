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
package io.trino.plugin.raptor.legacy.storage.organization;

import org.jdbi.v3.core.mapper.reflect.ColumnName;

import java.util.OptionalLong;

import static java.util.Objects.requireNonNull;

public class TableOrganizationInfo
{
    private final long tableId;
    private final OptionalLong lastStartTimeMillis;

    public TableOrganizationInfo(long tableId, @ColumnName("last_start_time") OptionalLong lastStartTimeMillis)
    {
        this.tableId = tableId;
        this.lastStartTimeMillis = requireNonNull(lastStartTimeMillis, "lastStartTimeMillis is null");
    }

    public long getTableId()
    {
        return tableId;
    }

    public OptionalLong getLastStartTimeMillis()
    {
        return lastStartTimeMillis;
    }
}
