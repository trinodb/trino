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
package io.trino.plugin.exasol.config;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class ExasolConfig
{
    private boolean mapHashTypeAsChar;

    public boolean isMapHashTypeAsChar()
    {
        return mapHashTypeAsChar;
    }

    @Config("exasol.legacy.map-hashtype-as-char")
    @ConfigDescription("Map Exasol HashType as char instead of varbinary")
    public ExasolConfig setMapHashTypeAsChar(boolean mapHashTypeAsChar)
    {
        this.mapHashTypeAsChar = mapHashTypeAsChar;
        return this;
    }
}
