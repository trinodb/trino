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
package io.prestosql.plugin.hive.metastore;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class TableWithPrivileges
{
    private final Table table;
    private final PrincipalPrivileges principalPrivileges;

    public TableWithPrivileges(Table table, PrincipalPrivileges principalPrivileges)
    {
        this.table = requireNonNull(table, "table is null");
        this.principalPrivileges = requireNonNull(principalPrivileges, "principalPrivileges is null");
    }

    public Table getTable()
    {
        return table;
    }

    public PrincipalPrivileges getPrincipalPrivileges()
    {
        return principalPrivileges;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("table", table)
                .add("principalPrivileges", principalPrivileges)
                .toString();
    }
}
