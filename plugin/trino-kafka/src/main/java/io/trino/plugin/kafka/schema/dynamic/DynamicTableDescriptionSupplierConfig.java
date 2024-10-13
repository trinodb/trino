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
package io.trino.plugin.kafka.schema.dynamic;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import jakarta.validation.constraints.NotNull;

import java.util.regex.Pattern;

public class DynamicTableDescriptionSupplierConfig
{
    private Pattern tableNameRegex;
    private long tableNameCacheTTLSecs = 60;

    @NotNull
    public Pattern getTableNameRegex()
    {
        return tableNameRegex;
    }

    @Config("kafka.table-name-regex")
    @ConfigDescription("Regex used to match against topic names, will create table for each one that matches.")
    public DynamicTableDescriptionSupplierConfig setTableNameRegex(String tableNameRegex)
    {
        this.tableNameRegex = Pattern.compile(tableNameRegex);
        return this;
    }

    public long getTableNameCacheTTLSecs()
    {
        return tableNameCacheTTLSecs;
    }

    @Config("kafka.table-name-cache-ttl-secs")
    @ConfigDescription("TTL of table cache in seconds, defaults to 60 secs.")
    public DynamicTableDescriptionSupplierConfig setTableNameCacheTTLSecs(int seconds)
    {
        this.tableNameCacheTTLSecs = seconds;
        return this;
    }
}
