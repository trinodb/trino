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
package io.trino.plugin.lance.catalog.namespace;

import io.airlift.configuration.Config;
import jakarta.validation.constraints.NotEmpty;

public class DirectoryNamespaceConfig
{
    private String warehouseLocation;

    @NotEmpty
    public String getWarehouseLocation()
    {
        return warehouseLocation;
    }

    @Config("lance.namespace.directory.warehouse.location")
    public DirectoryNamespaceConfig setWarehouseLocation(String warehouseLocation)
    {
        this.warehouseLocation = warehouseLocation;
        return this;
    }
}
