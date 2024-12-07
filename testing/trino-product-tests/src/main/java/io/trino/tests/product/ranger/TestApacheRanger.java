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
package io.trino.tests.product.ranger;

import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryExecutionException;
import io.trino.tempto.query.QueryExecutor;
import org.testng.annotations.Test;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.TestGroups.RANGER;
import static io.trino.tests.product.utils.QueryExecutors.connectToTrino;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestApacheRanger
        extends ProductTest
{
    @Test(groups = {RANGER, PROFILE_SPECIFIC_TESTS})
    public void testCreateTableAsSelect()
    {
        String tableName = "mariadb.test.nation_" + randomNameSuffix();

        // onTrino() is mapped to user hive. Ranger plugin is configured with hive as a superuser, so all queries from hive should succeed.
        try (QueryExecutor trino = onTrino()) {
            try {
                trino.executeQuery("DROP TABLE IF EXISTS " + tableName);
                assertThat(trino.executeQuery("CREATE TABLE " + tableName + " AS SELECT * FROM tpch.tiny.nation")).updatedRowsCountIsEqualTo(25);
                assertThat(trino.executeQuery("SELECT COUNT(*) FROM " + tableName)).containsOnly(row(25));
                assertThat(trino.executeQuery("TRUNCATE TABLE " + tableName)).updatedRowsCountIsEqualTo(0);
                assertThat(trino.executeQuery("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation")).updatedRowsCountIsEqualTo(25);
                assertThat(trino.executeQuery("UPDATE " + tableName + " SET comment = 'updated comment'")).updatedRowsCountIsEqualTo(25);
                assertThat(trino.executeQuery("DELETE FROM " + tableName)).updatedRowsCountIsEqualTo(25);

                // config 'alice@trino' is mapped to user alice. This user doesn't have any permissions in Ranger, so all queries should fail.
                try (QueryExecutor userAlice = connectToTrino("alice@trino")) {
                    assertThatThrownBy(() -> userAlice.executeQuery("SELECT COUNT(*) FROM " + tableName)).isInstanceOf(QueryExecutionException.class);
                    assertThatThrownBy(() -> userAlice.executeQuery("TRUNCATE TABLE " + tableName)).isInstanceOf(QueryExecutionException.class);
                    assertThatThrownBy(() -> userAlice.executeQuery("INSERT INTO " + tableName + " SELECT * FROM tpch.tiny.nation")).isInstanceOf(QueryExecutionException.class);
                    assertThatThrownBy(() -> userAlice.executeQuery("UPDATE " + tableName + " SET comment = 'updated comment'")).isInstanceOf(QueryExecutionException.class);
                    assertThatThrownBy(() -> userAlice.executeQuery("DELETE FROM " + tableName)).isInstanceOf(QueryExecutionException.class);
                }
            }
            finally {
                trino.executeQuery("DROP TABLE IF EXISTS " + tableName);
            }
        }
    }
}
