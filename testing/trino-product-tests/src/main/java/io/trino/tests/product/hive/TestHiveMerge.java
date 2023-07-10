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

import io.trino.tempto.assertions.QueryAssert;
import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.hive.util.TemporaryHiveTable;
import org.testng.SkipException;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertQueryFailure;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.HIVE_TRANSACTIONAL;
import static io.trino.tests.product.hive.BucketingType.BUCKETED_V2;
import static io.trino.tests.product.hive.BucketingType.NONE;
import static io.trino.tests.product.hive.TestHiveTransactionalTable.TEST_TIMEOUT;
import static io.trino.tests.product.hive.TestHiveTransactionalTable.tableName;
import static io.trino.tests.product.hive.TestHiveTransactionalTable.verifySelectForTrinoAndHive;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class TestHiveMerge
        extends HiveProductTest
{
    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeSimpleSelect()
    {
        withTemporaryTable("merge_simple_select_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable("merge_simple_select_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                onTrino().executeQuery(sql);

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeSimpleSelectPartitioned()
    {
        withTemporaryTable("merge_simple_select_partitioned_target", true, true, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true, partitioned_by = ARRAY['address'])", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable("merge_simple_select_partitioned_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                onTrino().executeQuery(sql);

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT, dataProvider = "partitionedAndBucketedProvider")
    public void testMergeUpdateWithVariousLayouts(boolean partitioned, String bucketing)
    {
        BucketingType bucketingType = bucketing.isEmpty() ? NONE : BUCKETED_V2;
        withTemporaryTable("merge_update_with_various_formats", true, partitioned, bucketingType, targetTable -> {
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
            onHive().executeQuery(builder.toString());

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable));
            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Dave", "dates"), row("Lou", "limes"), row("Carol", "candles"));

            withTemporaryTable("merge_update_with_various_formats_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

                onTrino().executeQuery(sql);

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Dave", "dates"), row("Carol_Craig", "candles"), row("Joe", "jellybeans"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testMergeUnBucketedUnPartitionedFailure()
    {
        withTemporaryTable("merge_with_various_formats_failure", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Dave', 'dates'), ('Lou', 'limes'), ('Carol', 'candles')", targetTable));
            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Dave", "dates"), row("Lou", "limes"), row("Carol", "candles"));

            withTemporaryTable("merge_with_various_formats_failure_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchase VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchase) VALUES ('Craig', 'candles'), ('Len', 'limes'), ('Joe', 'jellybeans')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.purchase = s.purchase)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.purchase = 'limes' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_', s.customer)" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchase) VALUES(s.customer, s.purchase)";

                onTrino().executeQuery(sql);

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Dave", "dates"), row("Carol_Craig", "candles"), row("Joe", "jellybeans"));
            });
        });
    }

    @DataProvider
    public Object[][] partitionedAndBucketedProvider()
    {
        return new Object[][] {
                {false, "CLUSTERED BY (customer) INTO 3 BUCKETS"},
                {false, "CLUSTERED BY (purchase) INTO 4 BUCKETS"},
                {true, ""},
                {true, "CLUSTERED BY (customer) INTO 3 BUCKETS"},
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeMultipleOperationsUnbucketedUnpartitioned()
    {
        withTemporaryTable("merge_multiple", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR) WITH (transactional = true)", targetTable));
            testMergeMultipleOperationsInternal(targetTable, 32);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeMultipleOperationsUnbucketedPartitioned()
    {
        withTemporaryTable("merge_multiple", true, true, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (purchases INT, zipcode INT, spouse VARCHAR, address VARCHAR, customer VARCHAR) WITH (transactional = true, partitioned_by = ARRAY['address', 'customer'])", targetTable));
            testMergeMultipleOperationsInternal(targetTable, 32);
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeMultipleOperationsBucketedUnpartitioned()
    {
        withTemporaryTable("merge_multiple", true, false, BUCKETED_V2, targetTable -> {
            onHive().executeQuery(format("CREATE TABLE %s (customer STRING, purchases INT, zipcode INT, spouse STRING, address STRING)" +
                    "   CLUSTERED BY(customer, zipcode, address) INTO 4 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')", targetTable));
            testMergeMultipleOperationsInternal(targetTable, 32);
        });
    }

    private void testMergeMultipleOperationsInternal(String targetTable, int targetCustomerCount)
    {
        String originalInsertFirstHalf = IntStream.range(1, targetCustomerCount / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 1000, 91000, intValue, intValue))
                .collect(Collectors.joining(", "));
        String originalInsertSecondHalf = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 2000, 92000, intValue, intValue))
                .collect(Collectors.joining(", "));

        onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s, %s", targetTable, originalInsertFirstHalf, originalInsertSecondHalf));

        String firstMergeSource = IntStream.range(targetCustomerCount / 2, targetCustomerCount)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jill_%s', '%s Eop Ct')", intValue, 3000, 83000, intValue, intValue))
                .collect(Collectors.joining(", "));

        onTrino().executeQuery(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, firstMergeSource) +
                "    ON t.customer = s.customer" +
                "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases, zipcode = s.zipcode, spouse = s.spouse, address = s.address");

        QueryResult expectedResult = onTrino().executeQuery(format("SELECT * FROM (VALUES %s, %s) AS v(customer, purchases, zipcode, spouse, address)", originalInsertFirstHalf, firstMergeSource));
        verifyOnTrinoAndHiveFromQueryResults("SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable, expectedResult);

        String nextInsert = IntStream.range(targetCustomerCount, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('jack_%s', %s, %s, 'jan_%s', '%s Poe Ct')", intValue, 4000, 74000, intValue, intValue))
                .collect(Collectors.joining(", "));
        onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, zipcode, spouse, address) VALUES %s", targetTable, nextInsert));

        String secondMergeSource = IntStream.range(1, targetCustomerCount * 3 / 2)
                .mapToObj(intValue -> format("('joe_%s', %s, %s, 'jen_%s', '%s Poe Ct')", intValue, 5000, 85000, intValue, intValue))
                .collect(Collectors.joining(", "));

        onTrino().executeQuery(format("MERGE INTO %s t USING (SELECT * FROM (VALUES %s)) AS s(customer, purchases, zipcode, spouse, address)", targetTable, secondMergeSource) +
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

        expectedResult = onTrino().executeQuery(format("SELECT * FROM (VALUES %s, %s, %s) AS v(customer, purchases, zipcode, spouse, address)", updatedBeginning, updatedMiddle, updatedEnd));
        verifyOnTrinoAndHiveFromQueryResults("SELECT customer, purchases, zipcode, spouse, address FROM " + targetTable, expectedResult);
    }

    private void verifyOnTrinoAndHiveFromQueryResults(String sql, QueryResult expectedResult)
    {
        QueryResult trinoResult = onTrino().executeQuery(sql);
        assertThat(trinoResult).contains(getRowsFromQueryResult(expectedResult));
        QueryResult hiveResult = onHive().executeQuery(sql);
        assertThat(hiveResult).contains(getRowsFromQueryResult(expectedResult));
    }

    private List<QueryAssert.Row> getRowsFromQueryResult(QueryResult result)
    {
        return result.rows().stream().map(QueryAssert.Row::new).collect(toImmutableList());
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeSimpleQuery()
    {
        withTemporaryTable("merge_simple_query_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            onTrino().executeQuery(format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                    "    " +
                    "ON (t.customer = s.customer)" +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeAllInserts()
    {
        withTemporaryTable("merge_all_inserts", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable));

            onTrino().executeQuery(format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Carol', 9, 'Centreville'), ('Dave', 22, 'Darbyshire'))) AS s(customer, purchases, address)" +
                    "    " +
                    "ON (t.customer = s.customer)" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeSimpleQueryPartitioned()
    {
        withTemporaryTable("merge_simple_query_partitioned_target", true, true, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true, partitioned_by = ARRAY['address'])", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            String query = format("MERGE INTO %s t USING ", targetTable) +
                    "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                    "    " +
                    "ON (t.customer = s.customer)" +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                    "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";
            onTrino().executeQuery(query);

            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeAllColumnsUpdated()
    {
        withTemporaryTable("merge_all_columns_updated_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Devon'), ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge')", targetTable));

            withTemporaryTable("merge_all_columns_updated_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire'), ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Ed', 7, 'Etherville')", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET customer = CONCAT(t.customer, '_updated'), purchases = s.purchases + t.purchases, address = s.address");

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Dave_updated", 22, "Darbyshire"), row("Aaron_updated", 11, "Arches"), row("Bill", 7, "Buena"), row("Carol_updated", 12, "Centreville"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeAllMatchesDeleted()
    {
        withTemporaryTable("merge_all_matches_deleted_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable("merge_all_matches_deleted_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN DELETE");

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Bill", 7, "Buena"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000, dataProvider = "partitionedBucketedFailure")
    public void testMergeMultipleRowsMatchFails(String createTableSql)
    {
        withTemporaryTable("merge_all_matches_deleted_target", true, true, NONE, targetTable -> {
            onHive().executeQuery(format(createTableSql, targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Antioch')", targetTable));

            withTemporaryTable("merge_all_matches_deleted_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Adelphi'), ('Aaron', 8, 'Ashland')", sourceTable));

                assertQueryFailure(() -> onTrino().executeQuery(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED THEN UPDATE SET address = s.address"))
                        .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): One MERGE target table row matched more than one source row");

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Adelphi' THEN UPDATE SET address = s.address");
                verifySelectForTrinoAndHive("SELECT customer, purchases, address FROM " + targetTable, row("Aaron", 5, "Adelphi"), row("Bill", 7, "Antioch"));
            });
        });
    }

    @DataProvider
    public Object[][] partitionedBucketedFailure()
    {
        return new Object[][] {
                {"CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"},
                {"CREATE TABLE %s (customer STRING, purchases INT, address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')"},
                {"CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"},
                {"CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')"},
                {"CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) CLUSTERED BY (address) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')"}
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeFailingPartitioning()
    {
        String testDescription = "failing_merge";
        withTemporaryTable(format("%s_target", testDescription), true, true, NONE, targetTable -> {
            onHive().executeQuery(format("CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(format("%s_source", testDescription), true, true, NONE, sourceTable -> {
                onHive().executeQuery(format("CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                onTrino().executeQuery(sql);

                verifySelectForTrinoAndHive("SELECT customer, purchases, address FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeFailureWithDifferentPartitioning()
    {
        testMergeWithDifferentPartitioningInternal(
                "target_partitioned_source_partitioned_and_bucketed",
                "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')");
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000, dataProvider = "targetAndSourceWithDifferentPartitioning")
    public void testMergeWithDifferentPartitioning(String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        testMergeWithDifferentPartitioningInternal(testDescription, createTargetTableSql, createSourceTableSql);
    }

    private void testMergeWithDifferentPartitioningInternal(String testDescription, String createTargetTableSql, String createSourceTableSql)
    {
        withTemporaryTable(format("%s_target", testDescription), true, true, NONE, targetTable -> {
            onHive().executeQuery(format(createTargetTableSql, targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable(format("%s_source", testDescription), true, true, NONE, sourceTable -> {
                onHive().executeQuery(format(createSourceTableSql, sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                String sql = format("MERGE INTO %s t USING %s s ON (t.customer = s.customer)", targetTable, sourceTable) +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)";

                onTrino().executeQuery(sql);

                verifySelectForTrinoAndHive("SELECT customer, purchases, address FROM " + targetTable, row("Aaron", 11, "Arches"), row("Ed", 7, "Etherville"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"));
            });
        });
    }

    @DataProvider
    public Object[][] targetAndSourceWithDifferentPartitioning()
    {
        return new Object[][] {
                {
                        "target_partitioned_source_and_target_partitioned_and_bucketed",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                },
                {
                        "target_flat_source_partitioned_by_customer",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')"
                },
                {
                        "target_partitioned_by_customer_source_flat",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                },
                {
                        "target_bucketed_by_customer_source_flat",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT, address STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                },
                {
                        "target_partitioned_source_partitioned_and_bucketed",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                },
                {
                        "target_partitioned_target_partitioned_and_bucketed",
                        "CREATE TABLE %s (customer STRING, purchases INT) PARTITIONED BY (address STRING) CLUSTERED BY (customer) INTO 3 BUCKETS STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                        "CREATE TABLE %s (purchases INT, address STRING) PARTITIONED BY (customer STRING) STORED AS ORC TBLPROPERTIES ('transactional'='true')",
                }
        };
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeQueryWithStrangeCapitalization()
    {
        withTemporaryTable("test_without_aliases_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            onTrino().executeQuery(format("MERGE INTO %s t USING ", targetTable.toUpperCase(ENGLISH)) +
                    "(SELECT * FROM (VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville'))) AS s(customer, purchases, address)" +
                    "ON (t.customer = s.customer)" +
                    "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                    "    WHEN MATCHED THEN UPDATE SET purCHases = s.PurchaseS + t.pUrchases, aDDress = s.addrESs" +
                    "    WHEN NOT MATCHED THEN INSERT (CUSTOMER, purchases, addRESS) VALUES(s.custoMer, s.Purchases, s.ADDress)");

            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeWithoutTablesAliases()
    {
        withTemporaryTable("test_without_aliases_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (cusTomer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable("test_without_aliases_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Ed', 7, 'Etherville'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire')", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s USING %s", targetTable, sourceTable) +
                        format(" ON (%s.customer = %s.customer)", targetTable, sourceTable) +
                        format("    WHEN MATCHED AND %s.address = 'Centreville' THEN DELETE", sourceTable) +
                        format("    WHEN MATCHED THEN UPDATE SET purchases = %s.pURCHases + %s.pUrchases, aDDress = %s.addrESs", sourceTable, targetTable, sourceTable) +
                        format("    WHEN NOT MATCHED THEN INSERT (cusTomer, purchases, addRESS) VALUES(%s.custoMer, %s.Purchases, %s.ADDress)", sourceTable, sourceTable, sourceTable));

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeWithUnpredictablePredicates()
    {
        withTemporaryTable("test_without_aliases_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (cusTomer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 5, 'Antioch'), ('Bill', 7, 'Buena'), ('Carol', 3, 'Cambridge'), ('Dave', 11, 'Devon')", targetTable));

            withTemporaryTable("test_without_aliases_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 6, 'Arches'), ('Carol', 9, 'Centreville'), ('Dave', 11, 'Darbyshire'), ('Ed', 7, 'Etherville')", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer AND s.purchases < 10.2" +
                        "    WHEN MATCHED AND s.address = 'Centreville' THEN DELETE" +
                        "    WHEN MATCHED THEN UPDATE SET purchases = s.purchases + t.purchases, address = s.address" +
                        "    WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable,
                        row("Aaron", 11, "Arches"), row("Bill", 7, "Buena"), row("Dave", 11, "Darbyshire"), row("Dave", 11, "Devon"), row("Ed", 7, "Etherville"));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE" +
                        "    WHEN MATCHED" +
                        "        THEN UPDATE SET purchases = s.purchases + t.purchases, address = concat(t.address, '/', s.address)" +
                        "    WHEN NOT MATCHED" +
                        "        THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)");

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable,
                        row("Aaron", 17, "Arches/Arches"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire/Darbyshire"), row("Ed", 14, "Etherville/Etherville"));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES('Fred', 30, 'Franklin')", targetTable));
                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable,
                        row("Aaron", 17, "Arches/Arches"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire/Darbyshire"), row("Ed", 14, "Etherville/Etherville"), row("Fred", 30, "Franklin"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeWithSimplifiedUnpredictablePredicates()
    {
        withTemporaryTable("test_without_aliases_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address)" +
                    " VALUES ('Dave', 11, 'Devon'), ('Dave', 11, 'Darbyshire')", targetTable));

            withTemporaryTable("test_without_aliases_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Dave', 11, 'Darbyshire')", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        " ON t.customer = s.customer" +
                        "    WHEN MATCHED AND t.address <> 'Darbyshire' AND s.purchases * 2 > 20" +
                        "        THEN DELETE");

                // BUG: The actual row are [Dave, 11, Devon].  Why did the wrong one get deleted?
                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Dave", 11, "Darbyshire"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeCasts()
    {
        withTemporaryTable("merge_cast_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (col1 TINYINT, col2 SMALLINT, col3 INT, col4 BIGINT, col5 REAL, col6 DOUBLE) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s VALUES (1, 2, 3, 4, 5, 6)", targetTable));

            withTemporaryTable("test_without_aliases_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (col1 DOUBLE, col2 REAL, col3 BIGINT, col4 INT, col5 SMALLINT, col6 TINYINT) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s VALUES (2, 3, 4, 5, 6, 7)", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.col1 + 1 = s.col1)" +
                        "    WHEN MATCHED THEN UPDATE SET col1 = s.col1, col2 = s.col2, col3 = s.col3, col4 = s.col4, col5 = s.col5, col6 = s.col6");

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row(2, 3, 4, 5, 6.0, 7.0));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeSubqueries()
    {
        withTemporaryTable("merge_nation_target", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (nation_name, region_name) VALUES ('FRANCE', 'EUROPE'), ('ALGERIA', 'AFRICA'), ('GERMANY', 'EUROPE')", targetTable));

            withTemporaryTable("merge_nation_source", true, false, NONE, sourceTable -> {
                onTrino().executeQuery(format("CREATE TABLE %s (nation_name VARCHAR, region_name VARCHAR) WITH (transactional = true)", sourceTable));

                onTrino().executeQuery(format("INSERT INTO %s VALUES ('ALGERIA', 'AFRICA'), ('FRANCE', 'EUROPE'), ('EGYPT', 'MIDDLE EAST'), ('RUSSIA', 'EUROPE')", sourceTable));

                onTrino().executeQuery(format("MERGE INTO %s t USING %s s", targetTable, sourceTable) +
                        "    ON (t.nation_name = s.nation_name)" +
                        "    WHEN MATCHED AND t.nation_name > (SELECT name FROM tpch.tiny.region WHERE name = t.region_name AND name LIKE ('A%'))" +
                        "        THEN DELETE" +
                        "    WHEN NOT MATCHED AND s.region_name = 'EUROPE'" +
                        "        THEN INSERT VALUES(s.nation_name, (SELECT 'EUROPE'))");

                verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("FRANCE", "EUROPE"), row("GERMANY", "EUROPE"), row("RUSSIA", "EUROPE"));
            });
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = 60 * 60 * 1000)
    public void testMergeOriginalFilesTarget()
    {
        withTemporaryTable("region", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s WITH (transactional=true) AS TABLE tpch.tiny.region", targetTable));

            // This merge is illegal, because many nations have the same region
            assertQueryFailure(() -> onTrino().executeQuery(format("MERGE INTO %s r USING tpch.tiny.nation n", targetTable) +
                    "    ON r.regionkey = n.regionkey" +
                    "    WHEN MATCHED" +
                    "        THEN UPDATE SET comment = n.comment"))
                    .hasMessageMatching("\\QQuery failed (#\\E\\S+\\Q): One MERGE target table row matched more than one source row");

            onTrino().executeQuery(format("MERGE INTO %s r USING tpch.tiny.nation n", targetTable) +
                    "    ON r.regionkey = n.regionkey AND n.name = 'FRANCE'" +
                    "    WHEN MATCHED" +
                    "        THEN UPDATE SET name = 'EUROPEAN'");

            verifySelectForTrinoAndHive("SELECT name  FROM %s WHERE name LIKE('EU%%')".formatted(targetTable), row("EUROPEAN"));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testMergeOverManySplits()
    {
        withTemporaryTable("delete_select", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (orderkey bigint, custkey bigint, orderstatus varchar(1), totalprice double, orderdate date, orderpriority varchar(15), clerk varchar(15), shippriority integer, comment varchar(79)) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s SELECT * FROM tpch.\"sf0.1\".orders", targetTable));

            String sql = format("MERGE INTO %s t USING (SELECT * FROM tpch.\"sf0.1\".orders) s ON (t.orderkey = s.orderkey)", targetTable) +
                    " WHEN MATCHED AND mod(s.orderkey, 3) = 0 THEN UPDATE SET totalprice = t.totalprice + s.totalprice" +
                    " WHEN MATCHED AND mod(s.orderkey, 3) = 1 THEN DELETE";

            onTrino().executeQuery(sql);

            verifySelectForTrinoAndHive(format("SELECT count(*) FROM %s t WHERE mod(t.orderkey, 3) = 1", targetTable), row(0));
        });
    }

    @Test(groups = HIVE_TRANSACTIONAL, timeOut = TEST_TIMEOUT)
    public void testMergeFalseJoinCondition()
    {
        withTemporaryTable("join_false", true, false, NONE, targetTable -> {
            onTrino().executeQuery(format("CREATE TABLE %s (customer VARCHAR, purchases INT, address VARCHAR) WITH (transactional = true)", targetTable));

            onTrino().executeQuery(format("INSERT INTO %s (customer, purchases, address) VALUES ('Aaron', 11, 'Antioch'), ('Bill', 7, 'Buena')", targetTable));

            // Test a literal false
            onTrino().executeQuery("""
                    MERGE INTO %s t USING (VALUES ('Carol', 9, 'Centreville')) AS s(customer, purchases, address)
                      ON (FALSE)
                        WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                    """.formatted(targetTable));
            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"));

            // Test a constant-folded false expression
            onTrino().executeQuery("""
                    MERGE INTO %s t USING (VALUES ('Dave', 22, 'Darbyshire')) AS s(customer, purchases, address)
                      ON (t.customer != t.customer)
                        WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                    """.formatted(targetTable));
            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire"));

            onTrino().executeQuery("""
                    MERGE INTO %s t USING (VALUES ('Ed', 7, 'Etherville')) AS s(customer, purchases, address)
                      ON (23 - (12 + 10) > 1)
                        WHEN MATCHED THEN UPDATE SET customer = concat(s.customer, '_fooled_you')
                        WHEN NOT MATCHED THEN INSERT (customer, purchases, address) VALUES(s.customer, s.purchases, s.address)
                    """.formatted(targetTable));
            verifySelectForTrinoAndHive("SELECT * FROM " + targetTable, row("Aaron", 11, "Antioch"), row("Bill", 7, "Buena"), row("Carol", 9, "Centreville"), row("Dave", 22, "Darbyshire"), row("Ed", 7, "Etherville"));
        });
    }

    @DataProvider
    public Object[][] insertersProvider()
    {
        return new Object[][] {
                {false, Engine.HIVE, Engine.TRINO},
                {false, Engine.TRINO, Engine.TRINO},
                {true, Engine.HIVE, Engine.TRINO},
                {true, Engine.TRINO, Engine.TRINO},
        };
    }

    @DataProvider
    public Object[][] inserterAndDeleterProvider()
    {
        return new Object[][] {
                {Engine.HIVE, Engine.TRINO},
                {Engine.TRINO, Engine.TRINO},
                {Engine.TRINO, Engine.HIVE}
        };
    }

    void withTemporaryTable(String rootName, boolean transactional, boolean isPartitioned, BucketingType bucketingType, Consumer<String> testRunner)
    {
        if (transactional) {
            ensureTransactionalHive();
        }
        try (TemporaryHiveTable table = TemporaryHiveTable.temporaryHiveTable(tableName(rootName, isPartitioned, bucketingType))) {
            testRunner.accept(table.getName());
        }
    }

    private void ensureTransactionalHive()
    {
        if (getHiveVersionMajor() < 3) {
            throw new SkipException("Hive transactional tables are supported with Hive version 3 or above");
        }
    }
}
