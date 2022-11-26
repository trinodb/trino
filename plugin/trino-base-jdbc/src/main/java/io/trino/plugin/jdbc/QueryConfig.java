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
package io.trino.plugin.jdbc;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

public class QueryConfig
{
    private boolean reuseConnection = true;
    private boolean retryOpeningConnection;

    public boolean isReuseConnection()
    {
        return reuseConnection;
    }

    public boolean isRetryOpeningConnection()
    {
        return retryOpeningConnection;
    }

    @Config("query.reuse-connection")
    @ConfigDescription("Enables reusing JDBC connection for metadata queries to data source within a single Trino query")
    public QueryConfig setReuseConnection(boolean reuseConnection)
    {
        this.reuseConnection = reuseConnection;
        return this;
    }

    @Config("query.retry-opening-connection")
    @ConfigDescription("Enables retrying opening JDBC connection")
    public QueryConfig setRetryOpeningConnection(boolean retryOpeningConnection)
    {
        this.retryOpeningConnection = retryOpeningConnection;
        return this;
    }
}
