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
package io.trino.plugin.duckdb;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class DuckDbConfig
{
    private boolean queryPassthroughEnabled;

    public boolean isQueryPassthroughEnabled()
    {
        return queryPassthroughEnabled;
    }

    @Config("duckdb.query-passthrough.enabled")
    @ConfigDescription("Expose the system.query table function and the system.execute procedure, which pass SQL text directly to DuckDB")
    public DuckDbConfig setQueryPassthroughEnabled(boolean queryPassthroughEnabled)
    {
        this.queryPassthroughEnabled = queryPassthroughEnabled;
        return this;
    }
}
