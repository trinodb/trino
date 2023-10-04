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
package io.trino.tests;

import com.google.common.collect.ImmutableList;
import io.trino.connector.MockConnectorFactory;
import io.trino.spi.Plugin;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorFactory;

import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;

public class FailingMockConnectorPlugin
        implements Plugin
{
    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        return ImmutableList.of(
                MockConnectorFactory.builder()
                        .withName("failing_mock")
                        .withListSchemaNames(session -> {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Catalog is broken");
                        })
                        .withListTables((session, schema) -> {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Catalog is broken");
                        })
                        .withGetViews((session, prefix) -> {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Catalog is broken");
                        })
                        .withGetMaterializedViews((session, prefix) -> {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Catalog is broken");
                        })
                        .withListTablePrivileges((session, prefix) -> {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Catalog is broken");
                        })
                        .withStreamTableColumns((session, prefix) -> {
                            throw new TrinoException(GENERIC_INTERNAL_ERROR, "Catalog is broken");
                        })
                        .build());
    }
}
