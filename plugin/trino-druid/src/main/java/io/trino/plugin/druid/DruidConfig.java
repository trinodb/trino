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
package io.trino.plugin.druid;

import io.airlift.configuration.Config;

public class DruidConfig
{
    private long executionTimeout; // milliseconds

    private String sqlTimeZone;

    @Config("druid.execution-timeout")
    public DruidConfig setExecutionTimeout(long executionTimeout)
    {
        this.executionTimeout = executionTimeout;
        return this;
    }

    public long getExecutionTimeout()
    {
        return executionTimeout;
    }

    @Config("druid.sql-timezone")
    public DruidConfig setSqlTimeZone(String sqlTimeZone)
    {
        this.sqlTimeZone = sqlTimeZone;
        return this;
    }

    public String getSqlTimeZone()
    {
        return sqlTimeZone;
    }
}
