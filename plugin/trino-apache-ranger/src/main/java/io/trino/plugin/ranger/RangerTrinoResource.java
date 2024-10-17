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

import org.apache.ranger.plugin.policyengine.RangerAccessResourceImpl;

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

    public RangerTrinoResource()
    {
    }

    public RangerTrinoResource(String catalogName, String schema, String table)
    {
        setValue(KEY_CATALOG, catalogName);
        setValue(KEY_SCHEMA, schema);
        setValue(KEY_TABLE, table);
    }

    public RangerTrinoResource(String catalogName, String schema, String table, String column)
    {
        setValue(KEY_CATALOG, catalogName);
        setValue(KEY_SCHEMA, schema);
        setValue(KEY_TABLE, table);
        setValue(KEY_COLUMN, column);
    }
}
