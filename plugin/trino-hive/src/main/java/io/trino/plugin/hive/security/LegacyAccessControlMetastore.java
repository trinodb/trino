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

import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.ConnectorSecurityContext;

import java.util.Optional;

/**
 * Interface for accessing metastore information needed for implementing legacy flavor of
 * access control mechanism.
 */
public interface LegacyAccessControlMetastore
{
    Optional<Table> getTable(ConnectorSecurityContext context, String databaseName, String tableName);
}
