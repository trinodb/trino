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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.longProperty;

public final class CassandraSessionProperties
{
    private static final String SPLITS_PER_NODE = "splits_per_node";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public CassandraSessionProperties(CassandraClientConfig cassandraClientConfig)
    {
        sessionProperties = ImmutableList.of(
                longProperty(
                        SPLITS_PER_NODE,
                        "Number of splits per node. By default, the values from the system.size_estimates table are used.",
                        cassandraClientConfig.getSplitsPerNode().orElse(null),
                        false));
    }

    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static Optional<Long> getSplitsPerNode(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(SPLITS_PER_NODE, Long.class));
    }
}
