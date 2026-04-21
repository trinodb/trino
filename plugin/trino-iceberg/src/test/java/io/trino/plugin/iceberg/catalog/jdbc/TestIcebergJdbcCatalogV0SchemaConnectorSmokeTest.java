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
package io.trino.plugin.iceberg.catalog.jdbc;

import io.trino.plugin.iceberg.catalog.jdbc.IcebergJdbcCatalogConfig.SchemaVersion;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestIcebergJdbcCatalogV0SchemaConnectorSmokeTest
        extends BaseIcebergJdbcCatalogConnectorSmokeTest
{
    public TestIcebergJdbcCatalogV0SchemaConnectorSmokeTest()
    {
        super(SchemaVersion.V0);
    }

    @Test
    @Override
    public void testView()
    {
        assertThatThrownBy(super::testView)
                .hasMessageContaining("Schema version V0 does not support views");
    }

    @Test
    @Override
    public void testCommentView()
    {
        assertThatThrownBy(super::testCommentView)
                .hasMessageContaining("Schema version V0 does not support views");
    }

    @Test
    @Override
    public void testCommentViewColumn()
    {
        assertThatThrownBy(super::testCommentViewColumn)
                .hasMessageContaining("Schema version V0 does not support views");
    }

    @Test
    @Override
    void testUnsupportedViewDialect()
    {
        assertThatThrownBy(super::testUnsupportedViewDialect)
                .hasMessageContaining("Error processing metadata");
    }
}
