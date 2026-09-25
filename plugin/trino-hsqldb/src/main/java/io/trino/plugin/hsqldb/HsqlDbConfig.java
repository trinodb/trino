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
package io.trino.plugin.hsqldb;

import io.airlift.configuration.Config;

import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;

public class HsqlDbConfig
{
    // An empty character means that the table doesn't have a comment in HsqlDB
    static final String HSQLDB_NO_COMMENT = "";
    static final String HSQLDB_DEFAULT_TABLE_TYPE = "hsqldb.default_table_type";

    private boolean includeSystemTables;
    private HsqlDbTableType defaultTableType = HsqlDbTableType.CACHED;

    public boolean isIncludeSystemTables()
    {
        return includeSystemTables;
    }

    @Config("hsqldb.include-system-tables")
    public HsqlDbConfig setIncludeSystemTables(boolean includeSystemTables)
    {
        this.includeSystemTables = includeSystemTables;
        return this;
    }

    public HsqlDbTableType getDefaultTableType()
    {
        return defaultTableType;
    }

    @Config("hsqldb.default-table-type")
    public HsqlDbConfig setDefaultTableType(String defaultTableType)
    {
        checkTableTypeValue(defaultTableType);
        this.defaultTableType = HsqlDbTableType.valueOf(defaultTableType);
        return this;
    }

    public static void checkTableTypeValue(String defaultTableType)
    {
        checkArgument(isTableTypeValue(defaultTableType), "unsupported table type value %s", defaultTableType);
    }

    private static boolean isTableTypeValue(String defaultTableType)
    {
        return Arrays.stream(HsqlDbTableType.values()).anyMatch(x -> x.name().equals(defaultTableType));
    }
}
