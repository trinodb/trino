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
package io.trino.tests.product.scylla;

import com.datastax.oss.driver.api.core.CqlSession;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.configuration.Configuration;
import io.trino.tempto.query.QueryResult;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import javax.inject.Inject;

import java.net.InetSocketAddress;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.SCYLLA;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestScylla
        extends ProductTest
{
    @Inject
    private Configuration configuration;

    @BeforeTestWithContext
    public void setUp()
    {
        onScylla("DROP KEYSPACE IF EXISTS test");
        onScylla("CREATE KEYSPACE test WITH replication={'class':'SimpleStrategy', 'replication_factor':1}");
    }

    @Test(groups = {SCYLLA, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        onTrino().executeQuery("DROP TABLE IF EXISTS scylla.test.nation");
        QueryResult result = onTrino().executeQuery("CREATE TABLE scylla.test.nation AS SELECT * FROM tpch.tiny.nation");
        try {
            assertThat(result).updatedRowsCountIsEqualTo(25);
            assertThat(onTrino().executeQuery("SELECT COUNT(*) FROM scylla.test.nation"))
                    .containsOnly(row(25));
        }
        finally {
            onTrino().executeQuery("DROP TABLE scylla.test.nation");
        }
    }

    private void onScylla(@Language("SQL") String query)
    {
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(new InetSocketAddress(configuration.getStringMandatory("databases.scylla.host"), configuration.getIntMandatory("databases.scylla.port")))
                .withLocalDatacenter(configuration.getStringMandatory("databases.scylla.local_datacenter"))
                .build()) {
            session.execute(query);
        }
    }
}
