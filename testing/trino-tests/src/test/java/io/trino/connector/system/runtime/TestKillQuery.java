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
package io.trino.connector.system.runtime;

import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.MoreCollectors.toOptional;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.testing.TestingAccessControlManager.TestingPrivilegeType.KILL_QUERY;
import static io.trino.testing.TestingAccessControlManager.privilege;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertFalse;

@Test(singleThreaded = true)
public class TestKillQuery
        extends AbstractTestQueryFramework
{
    private final ExecutorService executor = Executors.newSingleThreadScheduledExecutor(threadsNamed(TestKillQuery.class.getSimpleName()));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session defaultSession = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession).build();
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");
        return queryRunner;
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test(timeOut = 60_000)
    public void testKillQuery()
            throws Exception
    {
        String testQueryId = "test_query_id_" + randomUUID().toString().replace("-", "");
        Future<?> queryFuture = executor.submit(() -> {
            getQueryRunner().execute(format("SELECT count(comment) as %s FROM tpch.sf100000.lineitem", testQueryId));
        });

        Optional<Object> queryIdValue = Optional.empty();
        while (queryIdValue.isEmpty()) {
            Thread.sleep(50);
            queryIdValue = computeActual(format(
                    "SELECT query_id FROM system.runtime.queries WHERE query LIKE '%%%s%%' AND query NOT LIKE '%%system.runtime.queries%%'",
                    testQueryId))
                    .getOnlyColumn()
                    .collect(toOptional());
        }
        String queryId = queryIdValue.get().toString();

        assertFalse(queryFuture.isDone());

        getQueryRunner().getAccessControl().deny(privilege("query", KILL_QUERY));
        try {
            assertThatThrownBy(() -> getQueryRunner().execute(getSession("other_user"), format("CALL system.runtime.kill_query('%s', 'should fail')", queryId)))
                    .hasMessageContaining("Cannot kill query");
        }
        finally {
            getQueryRunner().getAccessControl().reset();
        }

        getQueryRunner().execute(format("CALL system.runtime.kill_query('%s', 'because')", queryId));

        assertThatThrownBy(() -> queryFuture.get(1, TimeUnit.MINUTES))
                .isInstanceOf(ExecutionException.class)
                .hasMessageContaining("Query killed. Message: because");
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(getSession().getCatalog().get())
                .setSchema(getSession().getSchema().get())
                .setIdentity(Identity.ofUser(user))
                .build();
    }
}
