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
package io.trino.plugin.hive.security;

import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.security.RoleGrant;

import java.util.Optional;
import java.util.Set;

/**
 * Interface for accessing metastore information needed for implementing sql-standard flavor of
 * access control mechanism.
 */
public interface SqlStandardAccessControlMetastore
{
    Set<RoleGrant> listRoleGrants(ConnectorSecurityContext context, HivePrincipal principal);

    Set<HivePrivilegeInfo> listTablePrivileges(ConnectorSecurityContext context, String databaseName, String tableName, Optional<HivePrincipal> principal);

    Optional<Database> getDatabase(ConnectorSecurityContext context, String databaseName);
}
