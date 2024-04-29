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
package io.trino.plugin.lance.internal;

import com.google.inject.Inject;
import io.airlift.http.client.HttpClient;
import io.airlift.log.Logger;
import io.trino.plugin.lance.LanceConfig;
import io.trino.spi.Page;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import java.net.URI;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;


public class LanceClient
{
    private static final Logger LOG = Logger.get(LanceClient.class);
    private static final String APPLICATION_JSON = "application/json";

    private final List<URI> lanceDbUri;
    private final HttpClient httpClient;

    @Inject
    public LanceClient(
            LanceConfig config,
            @LanceBind HttpClient httpClient)
    {
        this.lanceDbUri = config.getLanceDbUri();
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
    }

    public List<SchemaTableName> listTables(ConnectorSession session, String schema) {
        return null;
    }

    public Map<String, ColumnHandle> getColumnHandlers(String tableName) {
        return null;
    }

    public ColumnMetadata getColumnMetadata(ColumnHandle columnHandle) {
        return null;
    }

    public Iterator<Page> readTable(LanceDynamicTable query) {
        // TODO: implement me
        return null;
    }

    
}
