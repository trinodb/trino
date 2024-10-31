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
package io.trino.plugin.ranger;

import com.google.common.collect.ImmutableList;
import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

class RangerTrinoResource
        extends RangerAccessResourceImpl
{
    public static final String KEY_CATALOG = "catalog";
    public static final String KEY_SCHEMA = "schema";
    public static final String KEY_TABLE = "table";
    public static final String KEY_COLUMN = "column";
    public static final String KEY_USER = "trinouser";
    public static final String KEY_PROCEDURE = "procedure";
    public static final String KEY_SYSTEM_PROPERTY = "systemproperty";
    public static final String KEY_SESSION_PROPERTY = "sessionproperty";
    public static final String KEY_SCHEMA_FUNCTION = "schemafunction";
    public static final String KEY_ROLE = "role";
    public static final String KEY_QUERY_ID = "queryid";
    public static final String KEY_SYSINFO = "sysinfo";

    public static RangerTrinoResource forUser(String userName)
    {
        return new RangerTrinoResource(KEY_USER, userName);
    }

    public static RangerTrinoResource forRole(String roleName)
    {
        return new RangerTrinoResource(KEY_ROLE, roleName);
    }

    public static RangerTrinoResource forQueryId(String queryId)
    {
        return new RangerTrinoResource(KEY_QUERY_ID, queryId);
    }

    public static RangerTrinoResource forSystemProperty(String propertyName)
    {
        return new RangerTrinoResource(KEY_SYSTEM_PROPERTY, propertyName);
    }

    public static RangerTrinoResource forSystemInformation()
    {
        return new RangerTrinoResource(KEY_SYSINFO, "*");
    }

    public static RangerTrinoResource forCatalog(String catalogName)
    {
        return new RangerTrinoResource(KEY_CATALOG, catalogName);
    }

    public static RangerTrinoResource forSchema(String catalogName, String schema)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        ret.setValue(KEY_CATALOG, catalogName);
        ret.setValue(KEY_SCHEMA, schema);

        return ret;
    }

    public static RangerTrinoResource forTable(String catalogName, String schema, String tableName)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        ret.setValue(KEY_CATALOG, catalogName);
        ret.setValue(KEY_SCHEMA, schema);
        ret.setValue(KEY_TABLE, tableName);

        return ret;
    }

    public static RangerTrinoResource forColumn(String catalogName, String schema, String tableName, String columnName)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        ret.setValue(KEY_CATALOG, catalogName);
        ret.setValue(KEY_SCHEMA, schema);
        ret.setValue(KEY_TABLE, tableName);
        ret.setValue(KEY_COLUMN, columnName);

        return ret;
    }

    public static List<RangerTrinoResource> forColumns(String catalogName, String schema, String tableName, Set<String> columns)
    {
        if (columns.isEmpty()) {
            return ImmutableList.of(forTable(catalogName, schema, tableName));
        }

        return columns.stream().map(columnName -> forColumn(catalogName, schema, tableName, columnName)).collect(Collectors.toList());
    }

    public static RangerTrinoResource forSchemaProcedure(String catalogName, String schema, String procedure)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        ret.setValue(KEY_CATALOG, catalogName);
        ret.setValue(KEY_SCHEMA, schema);
        ret.setValue(KEY_PROCEDURE, procedure);

        return ret;
    }

    public static RangerTrinoResource forSchemaFunction(String catalogName, String schema, String functionName)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        ret.setValue(KEY_CATALOG, catalogName);
        ret.setValue(KEY_SCHEMA, schema);
        ret.setValue(KEY_SCHEMA_FUNCTION, functionName);

        return ret;
    }

    public static RangerTrinoResource forSessionProperty(String catalogName, String propertyName)
    {
        RangerTrinoResource ret = new RangerTrinoResource();

        ret.setValue(KEY_CATALOG, catalogName);
        ret.setValue(KEY_SESSION_PROPERTY, propertyName);

        return ret;
    }

    private RangerTrinoResource()
    {
    }

    public RangerTrinoResource(String key, String value)
    {
        setValue(key, value);
    }
}
