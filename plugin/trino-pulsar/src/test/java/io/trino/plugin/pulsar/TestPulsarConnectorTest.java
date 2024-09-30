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
package io.trino.plugin.pulsar;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.testng.annotations.Test;

import static io.trino.plugin.pulsar.PulsarServer.*;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class TestPulsarConnectorTest
        extends BaseConnectorTest {
    protected PulsarServer pulsarServer;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception {
        pulsarServer = closeAfterClass(new PulsarServer(PulsarServer.DEFAULT_IMAGE_NAME));
        QueryRunner runner = PulsarQueryRunner.createPulsarQueryRunner(pulsarServer, ImmutableMap.of("http-server.http.port", "8080"));
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_CUSTOMER), PulsarServer.CUSTOMER, PulsarServer.Customer.class, 2);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_ORDERS), PulsarServer.ORDERS, PulsarServer.Orders.class, 3);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_LINEITEM), PulsarServer.LINEITEM, PulsarServer.LineItem.class, 4);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_NATION), PulsarServer.NATION, PulsarServer.Nation.class, 1);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_REGION), PulsarServer.REGION, PulsarServer.Region.class, 1);
        return runner;
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior) {
        switch (connectorBehavior) {
            case SUPPORTS_DELETE:
            case SUPPORTS_INSERT:
            case SUPPORTS_CREATE_SCHEMA:
            case SUPPORTS_CREATE_TABLE:
            case SUPPORTS_CREATE_TABLE_WITH_DATA:
            case SUPPORTS_RENAME_TABLE:
            case SUPPORTS_COMMENT_ON_COLUMN:
            case SUPPORTS_COMMENT_ON_TABLE:
            case SUPPORTS_TOPN_PUSHDOWN:
            case SUPPORTS_AGGREGATION_PUSHDOWN:
                return false;
            default:
                return super.hasBehavior(connectorBehavior);
        }
    }

    @Override
    public void testShowColumns() {
        MaterializedResult actualColumns = computeActual("SHOW COLUMNS FROM orders");

        assertEquals(actualColumns.getMaterializedRows().size(), 9);
        assertEquals(actualColumns.getMaterializedRows().get(0).getField(0), "clerk");
        assertEquals(actualColumns.getMaterializedRows().get(0).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(1).getField(0), "comment");
        assertEquals(actualColumns.getMaterializedRows().get(1).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(2).getField(0), "custkey");
        assertEquals(actualColumns.getMaterializedRows().get(2).getField(1), "bigint");
        assertEquals(actualColumns.getMaterializedRows().get(3).getField(0), "orderdate");
        assertEquals(actualColumns.getMaterializedRows().get(3).getField(1), "date");
        assertEquals(actualColumns.getMaterializedRows().get(4).getField(0), "orderkey");
        assertEquals(actualColumns.getMaterializedRows().get(4).getField(1), "bigint");
        assertEquals(actualColumns.getMaterializedRows().get(5).getField(0), "orderpriority");
        assertEquals(actualColumns.getMaterializedRows().get(5).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(6).getField(0), "orderstatus");
        assertEquals(actualColumns.getMaterializedRows().get(6).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(7).getField(0), "shippriority");
        assertEquals(actualColumns.getMaterializedRows().get(7).getField(1), "integer");
        assertEquals(actualColumns.getMaterializedRows().get(8).getField(0), "totalprice");
        assertEquals(actualColumns.getMaterializedRows().get(8).getField(1), "double");
    }

    @Override
    public void testDescribeTable() {
        MaterializedResult actualColumns = computeActual("DESCRIBE orders");

        assertEquals(actualColumns.getMaterializedRows().size(), 9);
        assertEquals(actualColumns.getMaterializedRows().get(0).getField(0), "clerk");
        assertEquals(actualColumns.getMaterializedRows().get(0).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(1).getField(0), "comment");
        assertEquals(actualColumns.getMaterializedRows().get(1).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(2).getField(0), "custkey");
        assertEquals(actualColumns.getMaterializedRows().get(2).getField(1), "bigint");
        assertEquals(actualColumns.getMaterializedRows().get(3).getField(0), "orderdate");
        assertEquals(actualColumns.getMaterializedRows().get(3).getField(1), "date");
        assertEquals(actualColumns.getMaterializedRows().get(4).getField(0), "orderkey");
        assertEquals(actualColumns.getMaterializedRows().get(4).getField(1), "bigint");
        assertEquals(actualColumns.getMaterializedRows().get(5).getField(0), "orderpriority");
        assertEquals(actualColumns.getMaterializedRows().get(5).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(6).getField(0), "orderstatus");
        assertEquals(actualColumns.getMaterializedRows().get(6).getField(1), "varchar");
        assertEquals(actualColumns.getMaterializedRows().get(7).getField(0), "shippriority");
        assertEquals(actualColumns.getMaterializedRows().get(7).getField(1), "integer");
        assertEquals(actualColumns.getMaterializedRows().get(8).getField(0), "totalprice");
        assertEquals(actualColumns.getMaterializedRows().get(8).getField(1), "double");
    }

    @Test
    public void testShowCreateSchema() {
        String schemaName = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE SCHEMA \"" + schemaName + "\""))
                .isEqualTo(format("CREATE SCHEMA %s.\"%s\"", getSession().getCatalog().orElseThrow(), schemaName));
    }

    @Test
    public void testShowCreateTable() {
        assertThat((String) computeActual("SHOW CREATE TABLE orders").getOnlyValue())
                // If the connector reports additional column properties, the expected value needs to be adjusted in the test subclass
                .matches("CREATE TABLE \\w+\\.\"\\w+\\/\\w+\"\\.orders \\Q(\n" +
                        "   clerk varchar COMMENT '[\"null\",\"string\"]',\n" +
                        "   comment varchar COMMENT '[\"null\",\"string\"]',\n" +
                        "   custkey bigint COMMENT '\"long\"',\n" +
                        "   orderdate date COMMENT '[\"null\",{\"type\":\"int\",\"logicalType\":\"date\"}]',\n" +
                        "   orderkey bigint COMMENT '\"long\"',\n" +
                        "   orderpriority varchar COMMENT '[\"null\",\"string\"]',\n" +
                        "   orderstatus varchar COMMENT '[\"null\",\"string\"]',\n" +
                        "   shippriority integer COMMENT '\"int\"',\n" +
                        "   totalprice double COMMENT '\"double\"'\n" +
                        ")");
    }

    @Test
    public void testSelectAll() {
        assertQuery("SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment  FROM orders");
    }

    @Test
    public void testListSchemaNames() {
        MaterializedResult actualColumns = computeActual("SHOW SCHEMAS in pulsar");

        assertEquals(actualColumns.getMaterializedRows().get(0).getField(0), "information_schema");
        assertEquals(actualColumns.getMaterializedRows().get(1).getField(0), "public/default");
        assertEquals(actualColumns.getMaterializedRows().get(2).getField(0), "public/functions");
        assertEquals(actualColumns.getMaterializedRows().get(3).getField(0), "pulsar/system");
        assertEquals(actualColumns.getMaterializedRows().get(4).getField(0), "sample/ns1");
    }

    @Test
    public void testQueryTableNotExist() {
        assertQueryFails("DESCRIBE \"wrongtenant/wrongnamespace\".orders", "line 1:1: Schema 'wrongtenant/wrongnamespace' does not exist");
    }

    @Test
    public void testGetTableMetadataWrongSchema() {
        assertQueryFails("SELECT * FROM \"public/default\".orderss", "line 1:15: Table 'pulsar.public/default.orderss' does not exist");
    }

    @Test
    public void testGetTableMetadataTableNoSchema() throws Exception {
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(pulsarServer.getPulsarAdminUrl()).build();
        pulsarAdmin.topics().createNonPartitionedTopic("persistent://public/default/trino-test-1");
        Thread.sleep(500);
        MaterializedResult actualColumns = computeActual("DESCRIBE \"public/default\".\"trino-test-1\"");

        assertEquals(actualColumns.getMaterializedRows().size(), 1);
        assertEquals(actualColumns.getMaterializedRows().get(0).getField(0), "__value__");
        assertEquals(actualColumns.getMaterializedRows().get(0).getField(1), "varbinary");
    }

    @Test
    public void testPushDown() {
        assertEquals(computeActual("SELECT count(*) from orders where orderdate between date '1994-01-01' and date '1994-12-31'").getOnlyValue(), Long.valueOf(2303));
    }
}
