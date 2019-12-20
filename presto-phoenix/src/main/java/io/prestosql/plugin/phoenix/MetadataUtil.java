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
package io.prestosql.plugin.phoenix;

import org.apache.phoenix.util.SchemaUtil;

import java.util.Optional;

import static io.prestosql.plugin.phoenix.PhoenixMetadata.DEFAULT_SCHEMA;
import static org.apache.phoenix.query.QueryConstants.NULL_SCHEMA_NAME;

public final class MetadataUtil
{
    private MetadataUtil() {}

    public static String getEscapedTableName(Optional<String> schema, String table)
    {
        return SchemaUtil.getEscapedTableName(toPhoenixSchemaName(schema).orElse(null), table);
    }

    public static Optional<String> toPhoenixSchemaName(Optional<String> prestoSchemaName)
    {
        return prestoSchemaName.map(schemaName -> DEFAULT_SCHEMA.equalsIgnoreCase(schemaName) ? NULL_SCHEMA_NAME : schemaName);
    }

    public static Optional<String> toPrestoSchemaName(Optional<String> phoenixSchemaName)
    {
        return phoenixSchemaName.map(schemaName -> schemaName.isEmpty() ? DEFAULT_SCHEMA : schemaName);
    }
}
