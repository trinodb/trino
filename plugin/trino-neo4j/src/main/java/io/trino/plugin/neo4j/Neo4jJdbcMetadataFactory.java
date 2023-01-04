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
package io.trino.plugin.neo4j;

import io.trino.plugin.jdbc.DefaultJdbcMetadataFactory;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcMetadata;
import io.trino.plugin.jdbc.JdbcQueryEventListener;

import javax.inject.Inject;

import java.util.Set;

public class Neo4jJdbcMetadataFactory
        extends DefaultJdbcMetadataFactory
{
    private JdbcClient jdbcClient;
    private Set<JdbcQueryEventListener> jdbcQueryEventListeners;

    @Inject
    public Neo4jJdbcMetadataFactory(JdbcClient jdbcClient, Set<JdbcQueryEventListener> jdbcQueryEventListeners)
    {
        super(jdbcClient, jdbcQueryEventListeners);
        this.jdbcClient = jdbcClient;
        this.jdbcQueryEventListeners = jdbcQueryEventListeners;
    }

    @Override
    protected JdbcMetadata create(JdbcClient transactionCachingJdbcClient)
    {
        return new Neo4jMetadata(this.jdbcClient, false, jdbcQueryEventListeners);
    }
}
