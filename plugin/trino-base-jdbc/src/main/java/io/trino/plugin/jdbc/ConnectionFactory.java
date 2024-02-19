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

import io.trino.spi.connector.ConnectorSession;
import jakarta.annotation.PreDestroy;

import java.sql.Connection;
import java.sql.SQLException;

@FunctionalInterface
public interface ConnectionFactory
        extends AutoCloseable
{
    /**
     * Order in which connection factories will wrap around each other
     * Lowest priority is the outermost wrapping.
     * Gaps in the numbering allows for injecting custom behaviour
     * in between the default ones provided by base-jdbc.
     * **/
    int STATISTICS_CONNECTION_FACTORY_PRIORITY = 1;
    int LAZY_CONNECTION_FACTORY_PRIORITY = 101;
    int RETRYING_CONNECTION_FACTORY_PRIORITY = 201;
    int REUSABLE_CONNECTION_FACTORY_PRIORITY = 301;
    int KEEP_ALIVE_CONNECTION_FACTORY_PRIORITY = 401;

    Connection openConnection(ConnectorSession session)
            throws SQLException;

    @Override
    @PreDestroy
    default void close()
            throws SQLException
    {}
}
