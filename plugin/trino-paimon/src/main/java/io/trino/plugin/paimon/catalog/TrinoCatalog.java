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
package io.trino.plugin.paimon.catalog;

import io.trino.plugin.paimon.PaimonConfig;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.paimon.table.Table;

import java.util.List;
import java.util.Optional;

/**
 * An interface to allow different Paimon catalog implementations in PaimonMetadata.
 * <p>
 * It mimics the Paimon catalog interface, with the following modifications:
 * <ul>
 *   <li>ConnectorSession is added at the front of each method signature, similar to Iceberg's TrinoCatalog</li>
 * </ul>
 */
public interface TrinoCatalog
{
    String DB_SUFFIX = ".db";

    boolean databaseExists(ConnectorSession session, String database);

    List<String> listDatabases(ConnectorSession session);

    Table loadTable(ConnectorSession session, SchemaTableName schemaTableName);

    List<String> listTables(ConnectorSession session, Optional<String> namespace);

    String warehouse();

    PaimonConfig config();
}
