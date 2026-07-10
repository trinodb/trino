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
package io.trino.plugin.vdm;

import static java.util.Objects.requireNonNull;

/**
 * vdm name
 *
 * @since 2023-04-06
 */
public class VdmName
{
    private final String vdmName;
    private final String catalogType;

    /**
     * construction
     *
     * @param vdmName vdm name
     * @param connectorName catalog type
     */
    public VdmName(String vdmName, String connectorName)
    {
        this.vdmName = requireNonNull(vdmName, "vdm name is null");
        this.catalogType = requireNonNull(connectorName, "connector name is null");
    }

    public String getCatalogType()
    {
        return catalogType;
    }

    public String getVdmName()
    {
        return vdmName;
    }
}
