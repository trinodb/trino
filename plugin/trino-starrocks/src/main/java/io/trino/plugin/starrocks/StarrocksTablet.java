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

package io.trino.plugin.starrocks;

import java.io.Serializable;
import java.util.List;

public class StarrocksTablet
        implements Serializable
{
    private static final long serialVersionUID = 1L;

    private List<String> routings;
    private int version;
    private long versionHash;
    private long schemaHash;

    public List<String> getRoutings()
    {
        return routings;
    }

    public void setRoutings(List<String> routings)
    {
        this.routings = routings;
    }

    public int getVersion()
    {
        return version;
    }

    public void setVersion(int version)
    {
        this.version = version;
    }

    public long getVersionHash()
    {
        return versionHash;
    }

    public void setVersionHash(long versionHash)
    {
        this.versionHash = versionHash;
    }

    public long getSchemaHash()
    {
        return schemaHash;
    }

    public void setSchemaHash(long schemaHash)
    {
        this.schemaHash = schemaHash;
    }
}
