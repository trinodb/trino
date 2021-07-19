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
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_CUSTOMER;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_LINEITEM;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_NATION;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_ORDERS;
import static io.trino.plugin.pulsar.PulsarServer.SELECT_FROM_REGION;

public class TestBasePulsarConnectorTest
        extends BaseConnectorTest
{
    protected PulsarServer pulsarServer;

    @AfterClass(alwaysRun = true)
    public void destroy()
    {
        if (pulsarServer != null) {
            pulsarServer.close();
        }
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return false;
    }

    @Override
    public void testCreateTable()
    {
        // Create table task will get table handle first which we need to check if topic exist in Pulsar
        // and if topic not found we'll throw TableNotFound exception.
    }

    @Override
    public void testCreateTableAsSelect()
    {
        // Create table task will get table handle first which we need to check if topic exist in Pulsar
        // and if topic not found we'll throw TableNotFound exception.
    }

    @Override
    public void testDropTableIfExists()
    {
        //Not supported.
    }

    @Override
    public void testInformationSchemaFiltering()
    {
        //Try to access non-exist topic.
    }

    @Override
    public void testShowColumns()
    {
        //Pulsar table contains internal column representing message metadata, causing test failure.
    }

    @Override
    public void testDescribeTable()
    {
        //Pulsar table contains internal column representing message metadata, causing test failure.
    }

    @Override
    public void testShowCreateTable()
    {
        //Pulsar table contains internal column representing message metadata, causing test failure.
    }

    @Override
    public void testSelectAll()
    {
        //Pulsar table contains internal column representing message metadata, so it'll has more col than expected.
    }

    @Override
    public void testSelectInformationSchemaColumns()
    {
        //Try to access non-exist topic.
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        pulsarServer = new PulsarServer(PulsarServer.DEFAULT_IMAGE_NAME);
        QueryRunner runner = PulsarQueryRunner.createPulsarQueryRunner(pulsarServer, ImmutableMap.of());
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_CUSTOMER), PulsarServer.CUSTOMER, PulsarServer.Customer.class);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_ORDERS), PulsarServer.ORDERS, PulsarServer.Orders.class);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_LINEITEM), PulsarServer.LINEITEM, PulsarServer.LineItem.class);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_NATION), PulsarServer.NATION, PulsarServer.Nation.class);
        pulsarServer.copyAndIngestTpchData(runner.execute(SELECT_FROM_REGION), PulsarServer.REGION, PulsarServer.Region.class);
        return runner;
    }
}
