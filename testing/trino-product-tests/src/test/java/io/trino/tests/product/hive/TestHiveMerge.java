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
import io.trino.testing.containers.environment.QueryResult;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ProductTest
@RequiresEnvironment(HiveTransactionalEnvironment.class)
@TestGroup.HiveTransactional
class TestHiveMerge
{
    private static final int TEST_TIMEOUT = 15 * 60 * 1000;

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeSimpleSelect(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_simple_select_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, "merge_simple_select_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                env.executeTrinoUpdate(sql);

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeSimpleSelectPartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_simple_select_partitioned_target", true, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true, partitioned_by = ARRAY['address'])", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, "merge_simple_select_partitioned_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                env.executeTrinoUpdate(sql);

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    static Stream<Arguments> partitionedAndBucketedProvider()
    {
        return Stream.of(
                Arguments.of(false, "CLUSTERED BY (customer) INTO 3 BUCKETS"),
                Arguments.of(false, "CLUSTERED BY (purchase) INTO 4 BUCKETS"),
                Arguments.of(true, ""),
                Arguments.of(true, "CLUSTERED BY (customer) INTO 3 BUCKETS"));
    }

    @ParameterizedTest
    @MethodSource("partitionedAndBucketedProvider")
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testMergeUpdateWithVariousLayouts(boolean partitioned, String bucketing, HiveTransactionalEnvironment env)
    {
        BucketingType bucketingType = bucketing.isEmpty() ? BucketingType.NONE : BucketingType.BUCKETED_V2;
        withTemporaryTable(env, "merge_update_with_various_formats", partitioned, bucketingType, targetTable -> {
            StringBuilder builder = new StringBuilder();
            builder.append("CREATE TABLE ")
                    .append(targetTable)
                    .append("(customer STRING");
            builder.append(partitioned ? ") PARTITIONED BY (" : ", ")
                    .append("purchase STRING) ");
            if (!bucketing.isEmpty()) {
                builder.append(bucketing);
            }
            builder.append(" STORED AS ORC TBLPROPERTIES ('transactional' = 'true')");
            env.executeHiveUpdate(builder.toString());

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Dave", "dates"), row("Lou", "limes"), row("Carol", "candles"));

            withTemporaryTable(env, "merge_update_with_various_formats_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

                env.executeTrinoUpdate(sql);

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Dave", "dates"), row("Carol_Craig", "candles"), row("Joe", "jellybeans"));
            });
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testMergeUnBucketedUnPartitionedFailure(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_with_various_formats_failure", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Dave", "dates"), row("Lou", "limes"), row("Carol", "candles"));

            withTemporaryTable(env, "merge_with_various_formats_failure_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

                env.executeTrinoUpdate(sql);

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Dave", "dates"), row("Carol_Craig", "candles"), row("Joe", "jellybeans"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeMultipleOperationsUnbucketedUnpartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_multiple", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR) WITH (transactional = true)", targetTable));
            testMergeMultipleOperationsInternal(env, targetTable, 32);
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeMultipleOperationsUnbucketedPartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_multiple", true, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR, customer VARCHAR) WITH (transactional = true, partitioned_by = ARRAY['address', 'customer'])", targetTable));
            testMergeMultipleOperationsInternal(env, targetTable, 32);
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeMultipleOperationsBucketedUnpartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_multiple", false, BucketingType.BUCKETED_V2, targetTable -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (customer STRING, purchases INT, zipcode INT, spouse STRING, address STRING)" +
                    "   CLUSTERED BY(customer, zipcode, address) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')", targetTable));
            testMergeMultipleOperationsInternal(env, targetTable, 32);
        });
    }

    private void testMergeMultipleOperationsInternal(HiveTransactionalEnvironment env, String targetTable, int targetCustomerCount)
    {
        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(Collectors.joining(", "));

        env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf));

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(Collectors.joining(", "));

        env.executeTrinoUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                "    ON t.customer = s.customer" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address");

        QueryResult expectedResult = env.executeTrino(format("SELECT * FROM (VALUES %s, %s) AS v(customer, purchases, zipcode, spouse, address)", originalInsertFirstHalf, firstMergeSource));
        verifyOnTrinoAndHiveFromQueryResults(env, "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable, expectedResult);

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));
        env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert));

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        env.executeTrinoUpdate(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
                "    ON t.customer = s.customer" +
                "    WHEN MATCHED AND t.zipcode = 91000 THEN DELETE" +
                "    WHEN MATCHED AND s.zipcode = 85000 THEN UPDATE SET zipcode = 60000" +
                "    WHEN MATCHED THEN UPDATE SET zipcode = s.zipcode, spouse = s.spouse, address = s.address" +
                "    WHEN NOT MATCHED THEN INSERT (customer, purchases, zipcode, spouse, address) VALUES(s.customer, s.purchases, s.zipcode, s.spouse, s.address)");

        String updatedBeginning = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 60000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedMiddle = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String updatedEnd = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));

        expectedResult = env.executeTrino(format("SELECT * FROM (VALUES %s, %s, %s) AS v(customer, purchases, zipcode, spouse, address)", updatedBeginning, updatedMiddle, updatedEnd));
        verifyOnTrinoAndHiveFromQueryResults(env, "SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable, expectedResult);
    }

    private void verifyOnTrinoAndHiveFromQueryResults(HiveTransactionalEnvironment env, String sql, QueryResult expectedResult)
    {
        QueryResult trinoResult = env.executeTrino(sql);
        assertThat(trinoResult).contains(getRowsFromQueryResult(expectedResult));
        QueryResult hiveResult = env.executeHive(sql);
        assertThat(hiveResult).contains(getRowsFromQueryResult(expectedResult));
    }

    private List<Row> getRowsFromQueryResult(QueryResult result)
    {
        return result.rows().stream().map(Row::fromList).collect(toImmutableList());
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeSimpleQuery(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_simple_query_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            env.executeTrinoUpdate(format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                    "    " +
                    "ON (t.customer = s.customer)" +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeAllInserts(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_all_inserts", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable));

            env.executeTrinoUpdate(format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'))) AS s(customer, purchases, address)" +
                    "    " +
                    "ON (t.customer = s.customer)" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire"));
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeSimpleQueryPartitioned(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_simple_query_partitioned_target", true, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true, partitioned_by = ARRAY['address'])", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            String query = format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                    "    " +
                    "ON (t.customer = s.customer)" +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
            env.executeTrinoUpdate(query);

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeAllColumnsUpdated(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_all_columns_updated_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable));

            withTemporaryTable(env, "merge_all_columns_updated_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_updated'), purchases = s.purchases + t.purchases, address = s.address");

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Dave_updated", 22, "Darbyshire"), row("Aaron_updated", 11, "Arches"), row("Bill", 7, "Buena"), row("Carol_updated", 12, "Centreville"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeAllMatchesDeleted(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_all_matches_deleted_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, "merge_all_matches_deleted_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN DELETE");

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Bill", 7, "Buena"));
            });
        });
    }

    static Stream<String> partitionedBucketedFailure()
    {
        return Stream.of(
                "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "CREATE TABLE %s (customer STRING, purchases INT, address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (address) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    }

    @ParameterizedTest
    @MethodSource("partitionedBucketedFailure")
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeMultipleRowsMatchFails(String createTableSql, HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_all_matches_deleted_target", true, BucketingType.NONE, targetTable -> {
            env.executeHiveUpdate(format(createTableSql, targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable));

            withTemporaryTable(env, "merge_all_matches_deleted_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Adelphi'), ('Aaron', 8, 'Ashland')", sourceTable));

                assertThatThrownBy(() -> env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET address = s.address"))
                        .hasMessageMatching(".*One MERGE target table row matched more than one source row.*");

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address");
                verifySelectForTrinoAndHive(env, "SELECT customer, purchases, address FROM " + targetTable, row("Aaron", 5, "Adelphi"), row("Bill", 7, "Antioch"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeFailingPartitioning(HiveTransactionalEnvironment env)
    {
        String testDescription = "failing_merge";
        withTemporaryTable(env, format("%s_target", testDescription), true, BucketingType.NONE, targetTable -> {
            env.executeHiveUpdate(format("CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, format("%s_source", testDescription), true, BucketingType.NONE, sourceTable -> {
                env.executeHiveUpdate(format("CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                env.executeTrinoUpdate(sql);

                verifySelectForTrinoAndHive(env, "SELECT customer, purchases, address FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeFailureWithDifferentPartitioning(HiveTransactionalEnvironment env)
    {
        testMergeWithDifferentPartitioningInternal(
                env,
                "target_partitioned_source_partitioned_and_bucketed",
                "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    }

    static Stream<Arguments> targetAndSourceWithDifferentPartitioning()
    {
        return Stream.of(
                Arguments.of(
                        "target_partitioned_source_and_target_partitioned_and_bucketed",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')"),
                Arguments.of(
                        "target_flat_source_partitioned_by_customer",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"),
                Arguments.of(
                        "target_partitioned_by_customer_source_flat",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"),
                Arguments.of(
                        "target_bucketed_by_customer_source_flat",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"),
                Arguments.of(
                        "target_partitioned_source_partitioned_and_bucketed",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')"),
                Arguments.of(
                        "target_partitioned_target_partitioned_and_bucketed",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"));
    }

    @ParameterizedTest
    @MethodSource("targetAndSourceWithDifferentPartitioning")
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeWithDifferentPartitioning(String testDescription, String createTargetTableSql, String createSourceTableSql, HiveTransactionalEnvironment env)
    {
        testMergeWithDifferentPartitioningInternal(env, testDescription, createTargetTableSql, createSourceTableSql);
    }

    private void testMergeWithDifferentPartitioningInternal(HiveTransactionalEnvironment env, String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        withTemporaryTable(env, format("%s_target", testDescription), true, BucketingType.NONE, targetTable -> {
            env.executeHiveUpdate(format(createTargetTableSql, targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, format("%s_source", testDescription), true, BucketingType.NONE, sourceTable -> {
                env.executeHiveUpdate(format(createSourceTableSql, sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                env.executeTrinoUpdate(sql);

                verifySelectForTrinoAndHive(env, "SELECT customer, purchases, address FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeQueryWithStrangeCapitalization(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_without_aliases_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            env.executeTrinoUpdate(format("MERGE INTO %s t USING ", targetTable.toUpperCase(ENGLISH)) +
                    "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                    "ON (t.customer = s.customer)" +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purCHases = s.PurchaseS + t.pUrchases, aDDress = s.addrESs" +
                    "    WHEN NOT MATCHED THEN INSERT (CUSTOMER, purchases, addRESS) VALUES(s.custoMer, s.Purchases, s.ADDress)");

            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeWithoutTablesAliases(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_without_aliases_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (cusTomer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, "test_without_aliases_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s USING %s", targetTable, sourceTable) +
                        format(" ON (%s.customer = %s.customer)", targetTable, sourceTable) +
                        format("    WHEN MATCHED AND %s.address = 'Centreville' THEN DELETE", sourceTable) +
                        format("    WHEN MATCHED THEN UPDATE SET purchases = %s.pURCHases + %s.pUrchases, aDDress = %s.addrESs", sourceTable, targetTable, sourceTable) +
                        format("    WHEN NOT MATCHED THEN INSERT (cusTomer, purchases, addRESS) VALUES(%s.custoMer, %s.Purchases, %s.ADDress)", sourceTable, sourceTable, sourceTable));

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeWithUnpredictablePredicates(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_without_aliases_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (cusTomer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(env, "test_without_aliases_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer AND s.purchases < 10.2" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable,
                        row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 11, "Darbyshire"), row("Dave", 11, "Devon"), row("Ed", 7, "Etherville"));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE" +
                        "    WHEN MATCHED" +
                        "        THEN UPDATE SET purchases = s.purchases + t.purchases, address = concat(t.address, '/', s.address)" +
                        "    WHEN NOT MATCHED" +
                        "        THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable,
                        row("Aaron", 17, "Arches/Arches"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire/Darbyshire"), row("Ed", 14, "Etherville/Etherville"));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES('Fred', 30, 'Franklin')", targetTable));
                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable,
                        row("Aaron", 17, "Arches/Arches"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire/Darbyshire"), row("Ed", 14, "Etherville/Etherville"), row("Fred", 30, "Franklin"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeWithSimplifiedUnpredictablePredicates(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "test_without_aliases_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address)" +
                    " VALUES ('Dave', 11, 'Devon'), ('Dave', 11, 'Darbyshire')", targetTable));

            withTemporaryTable(env, "test_without_aliases_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire')", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE");

                // BUG: The actual row are [Dave, 11, Devon].  Why did the wrong one get deleted?
                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Dave", 11, "Darbyshire"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeCasts(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_cast_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (col1 TINYINT, col2 SMALLINT, col3 INT, col4 BIGINT, col5 REAL, col6 DOUBLE) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s VALUES (1, 2, 3, 4, 5, 6)", targetTable));

            withTemporaryTable(env, "test_without_aliases_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (col1 DOUBLE, col2 REAL, col3 BIGINT, col4 INT, col5 SMALLINT, col6 TINYINT) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6, 7)", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.col1 + 1 = s.col1)" +
                        "    WHEN MATCHED THEN UPDATE SET col1 = s.col1, col2 = s.col2, col3 = s.col3, col4 = s.col4, col5 = s.col5, col6 = s.col6");

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row(2, 3, 4, 5, 6.0, 7.0));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeSubqueries(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "merge_nation_target", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable));

            withTemporaryTable(env, "merge_nation_source", false, BucketingType.NONE, sourceTable -> {
                env.executeTrinoUpdate(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR) WITH (transactional = true)", sourceTable));

                env.executeTrinoUpdate(format("INSERT INTO %s VALUES ('ALGERIA', 'AFRICA'), ('FRANCE', 'EUROPE'), ('EGYPT', 'MIDDLE EAST'), ('RUSSIA', 'EUROPE')", sourceTable));

                env.executeTrinoUpdate(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.nation_name = s.nation_name)" +
                        "    WHEN MATCHED AND t.nation_name > (SELECT name FROM tpch.tiny.region WHERE name = t.region_name AND name LIKE ('A%'))" +
                        "        THEN DELETE" +
                        "    WHEN NOT MATCHED AND s.region_name = 'EUROPE'" +
                        "        THEN INSERT VALUES(s.nation_name, (SELECT 'EUROPE'))");

                verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("FRANCE", "EUROPE"), row("GERMANY", "EUROPE"), row("RUSSIA", "EUROPE"));
            });
        });
    }

    @Test
    @Timeout(value = 60 * 60 * 1000, unit = MILLISECONDS)
    void testMergeOriginalFilesTarget(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "region", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s WITH (transactional=true) AS TABLE tpch.tiny.region", targetTable));

            // This merge is illegal, because many nations have the same region
            assertThatThrownBy(() -> env.executeTrinoUpdate(format("MERGE INTO %s r USING tpch.tiny.nation n", targetTable) +
                    "    ON r.regionkey = n.regionkey" +
                    "    WHEN MATCHED" +
                    "        THEN UPDATE SET comment = n.comment"))
                    .hasMessageMatching(".*One MERGE target table row matched more than one source row.*");

            env.executeTrinoUpdate(format("MERGE INTO %s r USING tpch.tiny.nation n", targetTable) +
                    "    ON r.regionkey = n.regionkey AND n.name = 'FRANCE'" +
                    "    WHEN MATCHED" +
                    "        THEN UPDATE SET name = 'EUROPEAN'");

            verifySelectForTrinoAndHive(env, "SELECT name  FROM %s WHERE name LIKE('EU%%')".formatted(targetTable), row("EUROPEAN"));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testMergeOverManySplits(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "delete_select", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (orderkey bigint, custkey bigint, orderstatus varchar(1), totalprice double, orderdate date, orderpriority varchar(15), clerk varchar(15), shippriority integer, comment varchar(79)) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s SELECT * FROM tpch.\"sf0.1\".orders", targetTable));

            String sql = format("MERGE INTO %s t USING (SELECT * FROM tpch.\"sf0.1\".orders) s ON (t.orderkey = s.orderkey)", targetTable) +
                    " WHEN MATCHED AND mod(s.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice" +
                    " WHEN MATCHED AND mod(s.orderkey, 3) = 1 THEN DELETE";

            env.executeTrinoUpdate(sql);

            verifySelectForTrinoAndHive(env, format("SELECT count(*) FROM %s t WHERE mod(t.orderkey, 3) = 1", targetTable), row(0L));
        });
    }

    @Test
    @Timeout(value = TEST_TIMEOUT, unit = MILLISECONDS)
    void testMergeFalseJoinCondition(HiveTransactionalEnvironment env)
    {
        withTemporaryTable(env, "join_false", false, BucketingType.NONE, targetTable -> {
            env.executeTrinoUpdate(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            env.executeTrinoUpdate(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable));

            // Test a literal false
            env.executeTrinoUpdate(
                    """
                    MERGE INTO %s t USING (VALUES ('Carol', 9, 'Centreville')) AS s(customer, purchases, address)
                      ON (FALSE)
                        WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                    """.formatted(targetTable));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"));

            // Test a constant-folded false expression
            env.executeTrinoUpdate(
                    """
                    MERGE INTO %s t USING (VALUES ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)
                      ON (t.customer != t.customer)
                        WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                    """.formatted(targetTable));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire"));

            env.executeTrinoUpdate(
                    """
                    MERGE INTO %s t USING (VALUES ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)
                      ON (23 - (12 + 10) > 1)
                        WHEN MATCHED THEN UPDATE SET customer = concat(s.customer, '_fooled_you')
                        WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                    """.formatted(targetTable));
            verifySelectForTrinoAndHive(env, "SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    // Helper methods

    private static void verifySelectForTrinoAndHive(HiveTransactionalEnvironment env, String select, Row... rows)
    {
        assertThat(env.executeTrino(select))
                .describedAs("onTrino")
                .containsOnly(rows);
        assertThat(env.executeHive(select))
                .describedAs("onHive")
                .containsOnly(rows);
    }

    private static String tableName(String testName, boolean isPartitioned, BucketingType bucketingType)
    {
        return format("test_%s_%b_%s_%s", testName, isPartitioned, bucketingType.name(), randomNameSuffix());
    }

    private static void withTemporaryTable(HiveTransactionalEnvironment env, String rootName, boolean isPartitioned, BucketingType bucketingType, Consumer<String> testRunner)
    {
        String fullTableName = tableName(rootName, isPartitioned, bucketingType);
        try {
            testRunner.accept(fullTableName);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + fullTableName);
        }
    }
}
