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
package io.trino.plugin.warp.extension.execution.debugtools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.execution.TaskData;

public class RowGroupCollectData
        extends TaskData
{
    private final String schema;
    private final String table;
    private final String column;
    private final long cookieKey;
    private final long limit;
    private final String commandName;

    @JsonCreator
    public RowGroupCollectData(@JsonProperty("schema") String schema,
            @JsonProperty("table") String table,
            @JsonProperty("column") String column,
            @JsonProperty("cookie_key") long cookieKey,
            @JsonProperty("limit") long limit,
            @JsonProperty("command-name") String commandName)
    {
        this.schema = schema;
        this.table = table;
        this.column = column;
        this.cookieKey = cookieKey;
        this.limit = limit;
        this.commandName = commandName;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public String getTable()
    {
        return table;
    }

    @JsonProperty
    public String getColumn()
    {
        return column;
    }

    @JsonProperty
    public String getCommandName()
    {
        return commandName;
    }

    @JsonProperty
    public long getCookieKey()
    {
        return cookieKey;
    }

    @JsonProperty
    public long getLimit()
    {
        return limit;
    }

    @Override
    protected String getTaskName()
    {
        return WorkerRowGroupTask.WORKER_ROW_GROUP_COLLECT_TASK_NAME;
    }
}
