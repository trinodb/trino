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
package io.trino.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.configuration.LegacyConfig;
import jakarta.validation.constraints.NotNull;

public class TypeHandlingJdbcConfig
{
    private UnsupportedTypeHandling unsupportedTypeHandling = UnsupportedTypeHandling.IGNORE;

    @NotNull
    public UnsupportedTypeHandling getUnsupportedTypeHandling()
    {
        return unsupportedTypeHandling;
    }

    @Config("unsupported-type-handling")
    @LegacyConfig("unsupported-type.handling-strategy")
    @ConfigDescription("Unsupported type handling strategy")
    public TypeHandlingJdbcConfig setUnsupportedTypeHandling(UnsupportedTypeHandling unsupportedTypeHandling)
    {
        this.unsupportedTypeHandling = unsupportedTypeHandling;
        return this;
    }
}
