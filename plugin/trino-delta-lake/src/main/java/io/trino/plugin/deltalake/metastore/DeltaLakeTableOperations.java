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
package io.trino.plugin.deltalake.metastore;

import io.trino.annotation.NotThreadSafe;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

@NotThreadSafe
public abstract class DeltaLakeTableOperations
{
    protected final ConnectorSession session;
    protected final SchemaTableName schemaTableName;

    public DeltaLakeTableOperations(ConnectorSession session, SchemaTableName schemaTableName)
    {
        this.session = requireNonNull(session, "session is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
    }

    /**
     * @throws io.trino.spi.connector.TableNotFoundException if the table does not exist
     */
    public abstract void commitToExistingTable(long version, String schemaString, Optional<String> tableComment);
}
