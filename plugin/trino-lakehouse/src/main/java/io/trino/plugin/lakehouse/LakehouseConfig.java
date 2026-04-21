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
package io.trino.plugin.lakehouse;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotNull;

import static io.trino.plugin.lakehouse.TableType.ICEBERG;

public class LakehouseConfig
{
    private TableType tableType = ICEBERG;

    @NotNull
    public TableType getTableType()
    {
        return tableType;
    }

    @Config("lakehouse.table-type")
    public LakehouseConfig setTableType(TableType tableType)
    {
        this.tableType = tableType;
        return this;
    }
}
