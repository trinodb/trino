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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;

/**
 * Tests for Hive transactional table insert operations.
 * <p>
 * Ported from the Tempto-based TestHiveTransactionalTableInsert.
 */
@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
@TestGroup.HiveTransactional
class TestHiveTransactionalTableInsert
{
    @ParameterizedTest
    @MethodSource("transactionalTableTypes")
    void testInsertIntoTransactionalTable(TransactionalTableType type, HiveTransactionalEnvironment env)
    {
        String tableName = "test_insert_into_transactional_table_" + type.name().toLowerCase(ENGLISH);

        env.executeHiveUpdate("" +
                "CREATE TABLE " + tableName + "(a bigint)" +
                "CLUSTERED BY(a) INTO 4 BUCKETS STORED AS ORC " +
                hiveTableProperties(type));

        try {
            env.executeTrinoUpdate("INSERT INTO hive.default." + tableName + " (a) VALUES (42)");
            assertThat(env.executeTrino("SELECT * FROM hive.default." + tableName))
                    .containsOnly(row(42L));
        }
        finally {
            env.executeHiveUpdate("DROP TABLE " + tableName);
        }
    }

    static Stream<TransactionalTableType> transactionalTableTypes()
    {
        return Stream.of(TransactionalTableType.ACID, TransactionalTableType.INSERT_ONLY);
    }

    private static String hiveTableProperties(TransactionalTableType transactionalTableType)
    {
        return transactionalTableType.getHiveTableProperties().stream()
                .collect(joining(",", "TBLPROPERTIES (", ")"));
    }
}
