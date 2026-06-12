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
import io.airlift.configuration.ConfigDescription;

public class DruidConfig
{
    private boolean useDefaultValueForNull;

    public boolean isUseDefaultValueForNull()
    {
        return useDefaultValueForNull;
    }

    @Config("druid.use-default-value-for-null")
    @ConfigDescription("Druid cluster runs with druid.generic.useDefaultValueForNull=true (legacy null handling); disables aggregation pushdown to preserve Trino NULL semantics")
    public DruidConfig setUseDefaultValueForNull(boolean useDefaultValueForNull)
    {
        this.useDefaultValueForNull = useDefaultValueForNull;
        return this;
    }
}
