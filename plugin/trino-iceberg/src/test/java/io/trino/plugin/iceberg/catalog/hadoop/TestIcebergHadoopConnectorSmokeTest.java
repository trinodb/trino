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
package io.trino.plugin.iceberg.catalog.hadoop;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.iceberg.IcebergQueryRunner;
import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.tpch.TpchTable;
import org.apache.iceberg.FileFormat;
import org.testng.annotations.Test;

import java.util.Map;

import static io.trino.tpch.TpchTable.LINE_ITEM;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

// Redundant over TestIcebergOrcConnectorTest, but exists to exercise BaseConnectorSmokeTest
// Some features like materialized views may be supported by Iceberg only.
public class TestIcebergHadoopConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder().setIcebergProperties(Map.of("iceberg.file-format", FileFormat.ORC.name(), "iceberg.catalog.type", "hadoop")).setInitialTables(ImmutableList.<TpchTable<?>>builder().addAll(REQUIRED_TPCH_TABLES).add(LINE_ITEM).build()).build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_CREATE_MATERIALIZED_VIEW:
            case SUPPORTS_CREATE_VIEW:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_RENAME_TABLE:
                return false;
            case SUPPORTS_RENAME_TABLE_ACROSS_SCHEMAS:
            case SUPPORTS_DELETE:
                return true;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Test
    @Override
    public void testRenameTable()
    {
        assertThatThrownBy(super::testRenameTable).hasStackTraceContaining("Cannot rename Hadoop tables");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchemas()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchemas).hasStackTraceContaining("Cannot rename Hadoop tables");
    }

    @Test
    @Override
    public void testMaterializedView()
    {
        assertThatThrownBy(super::testMaterializedView).hasStackTraceContaining("createMaterializedView is not supported by iceberg");
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region")).isEqualTo("" + "CREATE TABLE iceberg.tpch.region (\n" + "   regionkey bigint,\n" + "   name varchar,\n" + "   comment varchar\n" + ")\n" + "WITH (\n" + "   format = 'ORC'\n" + ")");
    }
}
