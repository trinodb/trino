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
package io.trino.metadata;

import java.util.Objects;
import java.util.Optional;

import static io.trino.metadata.MetadataUtil.checkCatalogName;
import static java.util.Objects.requireNonNull;

public record QualifiedObjectPrefix(
        String catalogName,
        Optional<String> schemaName,
        Optional<String> objectName)
{
    public QualifiedObjectPrefix
    {
        checkCatalogName(catalogName);
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(objectName, "objectName is null");
    }

    public boolean matches(QualifiedObjectName objectName)
    {
        return Objects.equals(catalogName, objectName.catalogName())
                && schemaName.map(schema -> Objects.equals(schema, objectName.schemaName())).orElse(true)
                && this.objectName.map(table -> Objects.equals(table, objectName.objectName())).orElse(true);
    }

    @Override
    public String toString()
    {
        return catalogName + '.' + schemaName.orElse("*") + '.' + objectName.orElse("*");
    }
}
