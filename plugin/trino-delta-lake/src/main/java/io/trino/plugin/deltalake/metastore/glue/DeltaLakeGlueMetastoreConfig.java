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
package io.trino.plugin.deltalake.metastore.glue;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class DeltaLakeGlueMetastoreConfig
{
    private boolean hideNonDeltaLakeTables;

    public boolean isHideNonDeltaLakeTables()
    {
        return hideNonDeltaLakeTables;
    }

    @Config("delta.hide-non-delta-lake-tables")
    @ConfigDescription("Hide non-Delta Lake tables in table listings")
    public DeltaLakeGlueMetastoreConfig setHideNonDeltaLakeTables(boolean hideNonDeltaLakeTables)
    {
        this.hideNonDeltaLakeTables = hideNonDeltaLakeTables;
        return this;
    }
}
