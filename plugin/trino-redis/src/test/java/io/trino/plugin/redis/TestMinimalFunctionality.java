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
package io.trino.plugin.redis;

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TableHandle;
import io.trino.security.AllowAllAccessControl;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
        extends AbstractTestMinimalFunctionality
{
    @Override
    protected Map<String, String> connectorProperties()
    {
        return ImmutableMap.of();
    }

    @Test
    public void testTableExists()
    {
        QualifiedObjectName name = new QualifiedObjectName("redis", "default", tableName);
        transaction(queryRunner.getTransactionManager(), new AllowAllAccessControl())
                .singleStatement()
                .execute(SESSION, session -> {
                    Optional<TableHandle> handle = queryRunner.getServer().getMetadata().getTableHandle(session, name);
                    assertTrue(handle.isPresent());
                });
    }

    @Test
    public void testTableHasData()
    {
        clearData();

        assertThat(assertions.query("SELECT count(1) FROM " + tableName))
                .matches("VALUES BIGINT '0'");

        int count = 1000;
        populateData(count);

        assertThat(assertions.query("SELECT count(1) FROM " + tableName))
                .matches("VALUES BIGINT '%s'".formatted(count));
    }

    @Test
    public void testStringValueWhereClauseHasData()
    {
        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", stringValueTableName, stringValueTableName)))
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, stringValueTableName)))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, tableName)))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testHashValueWhereClauseHasData()
    {
        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", hashValueTableName, hashValueTableName)))
                .matches("VALUES BIGINT '1'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, hashValueTableName)))
                .matches("VALUES BIGINT '2'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, tableName)))
                .matches("VALUES BIGINT '1'");
    }

    @Test
    public void testStringValueWhereClauseHasNoData()
    {
        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", stringValueTableName, tableName)))
                .matches("VALUES BIGINT '0'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", stringValueTableName, tableName, tableName)))
                .matches("VALUES BIGINT '0'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999' AND id = 1", stringValueTableName, stringValueTableName)))
                .matches("VALUES BIGINT '0'");
    }

    @Test
    public void testHashValueWhereClauseHasNoData()
    {
        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", hashValueTableName, tableName)))
                .matches("VALUES BIGINT '0'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", hashValueTableName, tableName, tableName)))
                .matches("VALUES BIGINT '0'");

        assertThat(assertions.query(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999' AND id = 1", hashValueTableName, hashValueTableName)))
                .matches("VALUES BIGINT '0'");
    }
}
