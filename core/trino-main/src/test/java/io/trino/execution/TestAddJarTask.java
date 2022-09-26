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
package io.trino.execution;

import io.trino.execution.warnings.WarningCollector;
import io.trino.spi.resourcegroups.ResourceGroupId;
import io.trino.sql.tree.AddJar;
import io.trino.testing.LocalQueryRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAddJarTask
{
    private final String jarPath = "/Users/xx/presto-udfs/target/";

    protected LocalQueryRunner queryRunner;

    @BeforeMethod
    public void setUp()
    {
        queryRunner = LocalQueryRunner.create(TEST_SESSION);
    }

    @AfterMethod
    public void tearDown()
    {
        if (queryRunner != null) {
            queryRunner.close();
        }
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testAddJar()
    {
        AddJarTask task = getAddJarTask();
        AddJar statement = new AddJar(jarPath, true);
        getFutureValue(task.execute(statement, createNewQuery(), emptyList(), WarningCollector.NOOP));
        assertTrue(queryRunner.getFunctionJarDynamicManager().getFunctionJarStore().exists(jarPath));
    }

    private void testAdd()
    {
        queryRunner.execute("add jar if not exists 'aa'");
    }

    private AddJarTask getAddJarTask()
    {
        return new AddJarTask(queryRunner.getFunctionJarDynamicManager());
    }

    private QueryStateMachine createNewQuery()
    {
        return QueryStateMachine.begin(
                Optional.empty(),
                "test",
                Optional.empty(),
                queryRunner.getDefaultSession(),
                URI.create("fake://uri"),
                new ResourceGroupId("test"),
                false,
                queryRunner.getTransactionManager(),
                queryRunner.getAccessControl(),
                directExecutor(),
                queryRunner.getMetadata(),
                WarningCollector.NOOP,
                Optional.empty());
    }
}
