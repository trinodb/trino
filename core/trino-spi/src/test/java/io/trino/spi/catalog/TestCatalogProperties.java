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
package io.trino.spi.catalog;

import com.google.common.collect.ImmutableMap;
import io.trino.spi.connector.CatalogVersion;
import io.trino.spi.connector.ConnectorName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestCatalogProperties
{
    @Test
    void testToStringDoesNotIncludeProperties()
    {
        CatalogProperties catalogProperties = new CatalogProperties(
                new CatalogName("example"),
                new CatalogVersion("v1"),
                new ConnectorName("postgresql"),
                ImmutableMap.of("connection-password", "secret"));

        assertThat(catalogProperties)
                .hasToString("CatalogProperties{name=example, version=v1, connectorName=postgresql}");
    }
}
