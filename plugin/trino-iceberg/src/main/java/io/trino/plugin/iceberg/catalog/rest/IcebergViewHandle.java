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
package io.trino.plugin.iceberg.catalog.rest;

import io.trino.spi.connector.ConnectorViewHandle;

import java.util.List;

import static java.util.Objects.requireNonNull;

// Self-describing so it can be recognized even by another TrinoRestCatalog instance (cross-catalog reference).
public record IcebergViewHandle(String name, List<String> namespace, String catalog, String uuid, int versionId)
        implements ConnectorViewHandle
{
    public IcebergViewHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(namespace, "namespace is null");
        requireNonNull(catalog, "catalog is null");
        requireNonNull(uuid, "uuid is null");
    }
}
