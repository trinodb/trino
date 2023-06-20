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
package io.trino.tests.product.kudu;

import io.trino.tempto.query.QueryResult;
import org.testng.annotations.Test;

import java.util.UUID;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tests.product.TestGroups.KUDU;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestKuduConnectoKerberosSmokeTest
{
    @Test(groups = {KUDU, PROFILE_SPECIFIC_TESTS})
    public void kerberosAuthTicketExpiryTest()
            throws InterruptedException
    {
        String kuduTable = "kudu.default.nation_" + UUID.randomUUID().toString().replace("-", "");
        String table = "tpch.tiny.nation";

        assertThat(onTrino().executeQuery(format("SELECT count(*) from %s", table))).containsExactlyInOrder(row(25));
        QueryResult result = onTrino().executeQuery(format("CREATE TABLE %s AS SELECT * FROM %s", kuduTable, table));
        try {
            assertThat(result).updatedRowsCountIsEqualTo(25);
            assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %s", kuduTable))).containsExactlyInOrder(row(25));
            // Kerberos tickets are configured to expire after 60 seconds, this should expire the ticket
            Thread.sleep(70_000L);
            assertThat(onTrino().executeQuery(format("SELECT count(*) FROM %s", kuduTable))).containsExactlyInOrder(row(25));
        }
        finally {
            onTrino().executeQuery(format("DROP TABLE %s", kuduTable));
        }
    }
}
