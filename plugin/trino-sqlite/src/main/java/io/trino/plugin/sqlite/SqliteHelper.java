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
package io.trino.plugin.sqlite;

import io.trino.spi.type.Type;

import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.jdbc.BaseJdbcClient.varcharLiteral;
import static io.trino.plugin.sqlite.SqliteConfig.SQLITE_DATA_TYPES;
import static java.util.Locale.ENGLISH;

public final class SqliteHelper
{
    private SqliteHelper() {}

    public static String fromRemoteSchemaName(ResultSet resultSet, String defaultSchema)
            throws SQLException
    {
        return Optional.ofNullable(resultSet.getString("TABLE_SCHEM")).orElse(defaultSchema);
    }

    public static String toRemoteIdentifier(String identifier)
    {
        return identifier;
    }

    public static String fromRemoteIdentifier(String identifier)
    {
        return identifier.toLowerCase(ENGLISH);
    }

    public static String getDefaultValue(Type columnType, String defaultValue)
    {
        return switch (columnType.getBaseName()) {
            case "varchar", "char" -> varcharLiteral(defaultValue);
            default -> defaultValue;
        };
    }

    public static int getSqliteDataType(Map<String, String> customDataTypes, boolean useTypeAffinity, ResultSet resultSet, Optional<String> typeName)
            throws SQLException
    {
        int dataType = resultSet.getInt("DATA_TYPE");
        return getSqliteDataType(customDataTypes, useTypeAffinity, dataType, typeName);
    }

    public static int getSqliteDataType(Map<String, String> customDataTypes, boolean useTypeAffinity, int dataType, Optional<String> typeName)
    {
        // Here we aim to follow the logic of defining column affinities in SQLite.
        // If the type is not specified in the column definition of a table,
        // which is possible with SQLite, then TYPENAME can be empty and will be considered as BLOB.
        if (typeName.isPresent() && !typeName.get().isEmpty()) {
            if (SQLITE_DATA_TYPES.contains(typeName.get())) {
                return JDBCType.valueOf(typeName.get()).getVendorTypeNumber();
            }
            return getCustomDataType(customDataTypes, useTypeAffinity, typeName.get(), dataType);
        }
        return Types.BLOB;
    }

    public static Optional<String> getSqliteTypeName(Optional<String> typeName, int dataType)
    {
        if (typeName.isEmpty() || typeName.get().isEmpty()) {
            return Optional.of(JDBCType.valueOf(dataType).getName());
        }
        return typeName;
    }

    private static int getCustomDataType(Map<String, String> customDataTypes, boolean useTypeAffinity, String typeName, int dataType)
    {
        if (customDataTypes.containsKey(typeName)) {
            return JDBCType.valueOf(customDataTypes.get(typeName)).getVendorTypeNumber();
        }
        // If the type is not recognized and type affinity is not used as by default,
        // then that type is not handled in Trino and the value NULL is used for this purpose.
        return useTypeAffinity ? dataType : Types.NULL;
    }
}
