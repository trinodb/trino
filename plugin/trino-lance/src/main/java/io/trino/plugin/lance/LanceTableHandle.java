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
package io.trino.plugin.lance;

import io.trino.plugin.lance.metadata.Manifest;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;

import static java.util.Objects.requireNonNull;

public record LanceTableHandle(SchemaTableName name, Manifest manifest, String tablePath)
        implements ConnectorTableHandle
{
    public LanceTableHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(manifest, "manifest is null");
    }

    @Override
    public String toString()
    {
        return name.toString();
    }
}
