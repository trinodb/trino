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
package io.trino.plugin.exasol;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.session.PropertyMetadata.integerProperty;

public class ExasolSessionProperties
        implements SessionPropertiesProvider
{
    public static final String PARALLEL_CONNECTIONS_WORKER_COUNT = "parallel_connections_worker_count";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public ExasolSessionProperties(ExasolConfig exasolConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(integerProperty(
                        PARALLEL_CONNECTIONS_WORKER_COUNT,
                        "Maximum number of workers to use for parallel JDBC import. Set to 0 to deactivate parallel import.",
                        exasolConfig.getParallelConnectionsWorkerCount(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static int getParallelImportWorkerCount(ConnectorSession session)
    {
        return Optional.ofNullable(session.getProperty(PARALLEL_CONNECTIONS_WORKER_COUNT, Integer.class)).orElse(0);
    }
}
