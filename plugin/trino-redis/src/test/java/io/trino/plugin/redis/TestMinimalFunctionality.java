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
import io.trino.spi.type.BigintType;
import io.trino.testing.MaterializedResult;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.testing.assertions.Assert.assertEquals;
import static io.trino.transaction.TransactionBuilder.transaction;
import static java.lang.String.format;
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

        MaterializedResult result = queryRunner.execute("SELECT count(1) FROM " + tableName);
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        int count = 1000;
        populateData(count);

        result = queryRunner.execute("SELECT count(1) FROM " + tableName);
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row((long) count)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testStringValueWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", stringValueTableName, stringValueTableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, stringValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", stringValueTableName, stringValueTableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseHasData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", hashValueTableName, hashValueTableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, hashValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(2L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", hashValueTableName, hashValueTableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(1L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testStringValueWhereClauseHasNoData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", stringValueTableName, tableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", stringValueTableName, tableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999' AND id = 1", stringValueTableName, stringValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);
    }

    @Test
    public void testHashValueWhereClauseHasNoData()
    {
        MaterializedResult result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999'", hashValueTableName, tableName));
        MaterializedResult expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key IN ('%s:0', '%s:999')", hashValueTableName, tableName, tableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);

        result = queryRunner.execute(format("SELECT count(1) FROM %s WHERE redis_key = '%s:999' AND id = 1", hashValueTableName, hashValueTableName));
        expected = MaterializedResult.resultBuilder(SESSION, BigintType.BIGINT)
                .row(0L)
                .build();
        assertEquals(result, expected);
    }
}
