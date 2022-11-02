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
package io.trino.plugin.kudu;

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;

import java.util.Optional;

import static io.trino.plugin.kudu.KuduQueryRunnerFactory.createKuduQueryRunnerTpch;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseKuduConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    private TestingKuduServer kuduServer;

    protected abstract String getKuduServerVersion();

    protected abstract Optional<String> getKuduSchemaEmulationPrefix();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kuduServer = new TestingKuduServer(getKuduServerVersion());
        return createKuduQueryRunnerTpch(kuduServer, getKuduSchemaEmulationPrefix(), REQUIRED_TPCH_TABLES);
    }

    @SuppressWarnings("DuplicateBranchesInSwitch")
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        switch (connectorBehavior) {
            case SUPPORTS_TOPN_PUSHDOWN:
                return false;

            case SUPPORTS_RENAME_SCHEMA:
                return false;

            case SUPPORTS_CREATE_TABLE_WITH_TABLE_COMMENT:
                return false;

            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
                return false;

            case SUPPORTS_NOT_NULL_CONSTRAINT:
                return false;

            case SUPPORTS_DELETE:
            case SUPPORTS_MERGE:
                return true;

            case SUPPORTS_ARRAY:
            case SUPPORTS_ROW_TYPE:
            case SUPPORTS_NEGATIVE_DATE:
                return false;

            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    protected String getCreateTableDefaultDefinition()
    {
        return "(a bigint WITH (primary_key=true), b double) " +
                "WITH (partition_by_hash_columns = ARRAY['a'], partition_by_hash_buckets = 2)";
    }

    @Override
    public void testShowCreateTable()
    {
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .isEqualTo("CREATE TABLE kudu." + getSession().getSchema().orElseThrow() + ".region (\n" +
                        "   regionkey bigint COMMENT '' WITH ( nullable = true ),\n" +
                        "   name varchar COMMENT '' WITH ( nullable = true ),\n" +
                        "   comment varchar COMMENT '' WITH ( nullable = true )\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   number_of_replicas = 3,\n" +
                        "   partition_by_hash_buckets = 2,\n" +
                        "   partition_by_hash_columns = ARRAY['row_uuid'],\n" +
                        "   partition_by_range_columns = ARRAY['row_uuid'],\n" +
                        "   range_partitions = '[{\"lower\":null,\"upper\":null}]'\n" +
                        ")");
    }
}
