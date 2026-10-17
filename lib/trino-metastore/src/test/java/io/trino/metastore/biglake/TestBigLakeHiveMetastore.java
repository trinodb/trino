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
package io.trino.metastore.biglake;

import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.StorageDescriptor;
import io.airlift.slice.Slices;
import io.trino.metastore.AcidOperation;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBucketProperty;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.SortingColumn;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import org.assertj.core.api.ThrowableAssert;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static io.trino.metastore.MetastoreErrorCode.HIVE_INVALID_METADATA;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.metastore.biglake.TestingBigLakeObjects.field;
import static io.trino.metastore.biglake.TestingBigLakeObjects.partition;
import static io.trino.metastore.biglake.TestingBigLakeObjects.table;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.security.PrincipalType.USER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestBigLakeHiveMetastore
{
    private static final String SCHEMA = "sales";
    private static final String TABLE = "orders";

    private TestingBigLakeClient bigLake;
    private BigLakeHiveMetastore metastore;

    @BeforeEach
    void setUp()
    {
        bigLake = new TestingBigLakeClient();
        metastore = new BigLakeHiveMetastore(() -> bigLake);
    }

    // Schemas

    @Test
    void listsSchemaNames()
    {
        bigLake.add(TestingBigLakeObjects.database(SCHEMA)).add(TestingBigLakeObjects.database("logs"));

        assertThat(metastore.getAllDatabases()).containsExactly(SCHEMA, "logs");
    }

    @Test
    void reportsAnUnknownCatalogAsAnEmptyListOfSchemas()
    {
        assertThat(metastore.getAllDatabases()).isEmpty();
    }

    @Test
    void returnsASchema()
    {
        bigLake.add(TestingBigLakeObjects.database(SCHEMA));

        Optional<Database> database = metastore.getDatabase(SCHEMA);

        assertThat(database).isPresent();
        assertThat(database.get().getLocation()).contains("gs://warehouse/sales.db");
    }

    @Test
    void reportsAMissingSchemaAsEmptyRatherThanAnError()
    {
        assertThat(metastore.getDatabase("missing")).isEmpty();
    }

    @Test
    void surfacesAuthorizationFailuresInsteadOfHidingThemAsMissingSchemas()
    {
        bigLake.failWith(new TrinoException(PERMISSION_DENIED, "caller lacks BigLake access"));

        assertThatThrownBy(() -> metastore.getDatabase(SCHEMA))
                .isInstanceOf(TrinoException.class)
                .satisfies(thrown -> assertErrorCode(thrown, PERMISSION_DENIED));
    }

    // Tables

    @Test
    void listsTablesWithTheirRelationType()
    {
        bigLake.add(table(SCHEMA, TABLE, "", List.of(field("id", "bigint")), List.of()))
                .add(table(SCHEMA, "returns", "", List.of(field("id", "bigint")), List.of()));

        List<TableInfo> tables = metastore.getTables(SCHEMA);

        assertThat(tables)
                .extracting(tableInfo -> tableInfo.tableName().getTableName())
                .containsExactly(TABLE, "returns");
        // A listing returns whole tables, so the relation type costs no extra request
        assertThat(tables).allMatch(tableInfo -> tableInfo.extendedRelationType() == TableInfo.ExtendedRelationType.TABLE);
    }

    @Test
    void returnsATableWithItsColumnsPartitionKeysAndFormat()
    {
        bigLake.add(table(
                SCHEMA,
                TABLE,
                "daily orders",
                List.of(field("id", "bigint"), field("total", "decimal(10,2)")),
                List.of(field("region", "string"))));

        Table table = metastore.getTable(SCHEMA, TABLE).orElseThrow();

        assertThat(table.getDataColumns()).extracting(Column::getName).containsExactly("id", "total");
        assertThat(table.getPartitionColumns()).extracting(Column::getName).containsExactly("region");
        assertThat(table.getStorage().getStorageFormat().getSerde()).isEqualTo(TestingBigLakeObjects.PARQUET_SERDE);
        assertThat(table.getStorage().getLocation()).isEqualTo("gs://warehouse/orders");
        assertThat(table.getParameters()).containsEntry("comment", "daily orders");
        assertThat(table.getTableType()).isEqualTo("EXTERNAL_TABLE");
    }

    @Test
    void readsBucketingBecauseBigLakeReportsIt()
    {
        HiveTable bucketed = table(SCHEMA, TABLE, "", List.of(field("id", "bigint")), List.of());
        StorageDescriptor storage = bucketed.getStorageDescriptor().toBuilder()
                .setNumBuckets(8)
                .addBucketCols("id")
                .addSortCols(StorageDescriptor.Order.newBuilder().setCol("id").setOrder(1))
                .build();
        bigLake.add(bucketed.toBuilder().setStorageDescriptor(storage).build());

        Optional<HiveBucketProperty> bucketProperty =
                metastore.getTable(SCHEMA, TABLE).orElseThrow().getStorage().getBucketProperty();

        assertThat(bucketProperty).isPresent();
        assertThat(bucketProperty.get().bucketCount()).isEqualTo(8);
        assertThat(bucketProperty.get().bucketedBy()).containsExactly("id");
        assertThat(bucketProperty.get().sortedBy())
                .containsExactly(new SortingColumn("id", SortingColumn.Order.ASCENDING));
    }

    @Test
    void reportsAMissingTableAsEmptyRatherThanAnError()
    {
        assertThat(metastore.getTable(SCHEMA, "missing")).isEmpty();
    }

    @Test
    void rejectsATableWhoseColumnTypeCannotBeRead()
    {
        bigLake.add(table(SCHEMA, TABLE, "", List.of(field("id", "not_a_hive_type")), List.of()));

        assertThatThrownBy(() -> metastore.getTable(SCHEMA, TABLE))
                .isInstanceOf(TrinoException.class)
                .satisfies(thrown -> assertErrorCode(thrown, HIVE_INVALID_METADATA))
                .hasMessageContaining("id");
    }

    // Partitions

    @Test
    void namesPartitionsTheWayTrinoDoes()
    {
        partitionedOrders();
        bigLake.add(SCHEMA, TABLE, partition(TABLE, List.of("emea/north")));

        assertThat(partitionNames(TupleDomain.all())).containsExactly("region=emea%2Fnorth");
    }

    @Test
    void pushesDownAnEqualityOnAPartitionColumn()
    {
        partitionedOrders();
        bigLake.add(SCHEMA, TABLE, partition(TABLE, List.of("emea")))
                .add(SCHEMA, TABLE, partition(TABLE, List.of("apac")));

        TupleDomain<String> filter = TupleDomain.withColumnDomains(
                Map.of("region", Domain.singleValue(VARCHAR, Slices.utf8Slice("emea"))));

        assertThat(partitionNames(filter)).containsExactly("region=emea");
        assertThat(bigLake.partitionFilters()).containsExactly("region='emea'");
    }

    @Test
    void leavesAFilterItCannotPushDownToTrino()
    {
        partitionedOrders();
        bigLake.add(SCHEMA, TABLE, partition(TABLE, List.of("emea")));

        TupleDomain<String> filter = TupleDomain.withColumnDomains(Map.of(
                "region", Domain.create(ValueSet.ofRanges(Range.greaterThan(VARCHAR, Slices.utf8Slice("a"))), false)));

        assertThat(partitionNames(filter)).containsExactly("region=emea");
        assertThat(bigLake.partitionFilters()).containsExactly("");
    }

    @Test
    void leavesAValueThatWouldBreakTheFilterToTrino()
    {
        partitionedOrders();
        bigLake.add(SCHEMA, TABLE, partition(TABLE, List.of("emea")));

        TupleDomain<String> filter = TupleDomain.withColumnDomains(
                Map.of("region", Domain.singleValue(VARCHAR, Slices.utf8Slice("it's"))));

        assertThat(partitionNames(filter)).containsExactly("region=emea");
        assertThat(bigLake.partitionFilters()).containsExactly("");
    }

    @Test
    void looksUpAPartitionWhoseValueWouldBreakTheFilter()
    {
        Table table = partitionedOrders();
        bigLake.add(SCHEMA, TABLE, partition(TABLE, List.of("emea")))
                .add(SCHEMA, TABLE, partition(TABLE, List.of("o'brien")));

        assertThat(metastore.getPartition(table, List.of("o'brien")))
                .hasValueSatisfying(partition -> assertThat(partition.getValues()).containsExactly("o'brien"));
        assertThat(metastore.getPartition(table, List.of("o'neil"))).isEmpty();

        String partitionName = makePartName(List.of("region"), List.of("o'brien"));
        assertThat(metastore.getPartitionsByNames(table, List.of(partitionName)).get(partitionName))
                .hasValueSatisfying(partition -> assertThat(partition.getValues()).containsExactly("o'brien"));

        // A quote ends the literal and changes the filter, so no filter carries the value
        assertThat(bigLake.partitionFilters()).allSatisfy(filter -> assertThat(filter).doesNotContain("brien"));
    }

    @Test
    void answersAnImpossibleFilterWithoutCallingBigLake()
    {
        partitionedOrders();

        assertThat(partitionNames(TupleDomain.none())).isEmpty();
        assertThat(bigLake.partitionFilters()).isEmpty();
    }

    @Test
    void reportsAMissingTableWhenListingPartitionNames()
    {
        assertThat(metastore.getPartitionNamesByFilter(SCHEMA, "missing", List.of("region"), TupleDomain.all()))
                .isEmpty();
    }

    @Test
    void returnsAnEntryForEveryRequestedPartitionName()
    {
        Table table = partitionedOrders();
        bigLake.add(SCHEMA, TABLE, partition(TABLE, List.of("emea")));

        Map<String, Optional<Partition>> partitions =
                metastore.getPartitionsByNames(table, List.of("region=emea", "region=apac"));

        assertThat(partitions).containsOnlyKeys("region=emea", "region=apac");
        assertThat(partitions.get("region=emea")).isPresent();
        assertThat(partitions.get("region=apac")).isEmpty();
    }

    @Test
    void doesNotTurnABackendFailureIntoAnEmptyResult()
    {
        // A failure that reads as an empty result is worse than a failure: a query reports an empty
        // schema or a missing table instead of saying that BigLake cannot be reached.
        Table table = partitionedOrders();
        bigLake.failWith(new TrinoException(GENERIC_INTERNAL_ERROR, "BigLake is unavailable"));

        assertBackendFailure(metastore::getAllDatabases);
        assertBackendFailure(() -> metastore.getDatabase(SCHEMA));
        assertBackendFailure(() -> metastore.getTables(SCHEMA));
        assertBackendFailure(() -> metastore.getTable(SCHEMA, TABLE));
        assertBackendFailure(() -> metastore.getPartition(table, List.of("emea")));
        assertBackendFailure(() -> metastore.getPartitionNamesByFilter(SCHEMA, TABLE, List.of("region"), TupleDomain.all()));
        assertBackendFailure(() -> metastore.getPartitionsByNames(table, List.of("region=emea")));
    }

    private static void assertBackendFailure(ThrowableAssert.ThrowingCallable read)
    {
        assertThatThrownBy(read)
                .isInstanceOf(TrinoException.class)
                .satisfies(thrown -> assertErrorCode(thrown, GENERIC_INTERNAL_ERROR));
    }

    // Everything the read path does not implement

    @Test
    void doesNotReachBigLakeUntilItIsAskedSomething()
    {
        // Trino builds a metastore while a catalog loads. Reaching BigLake at that point stops a
        // server from starting whenever credentials are missing.
        BigLakeHiveMetastore lazyMetastore = new BigLakeHiveMetastore(() -> {
            throw new AssertionError("BigLake was reached before a metastore method was called");
        });

        assertThatThrownBy(lazyMetastore::getAllDatabases).isInstanceOf(AssertionError.class);
    }

    @Test
    void failsEveryWriteAndDdlOperationExplicitly()
    {
        Map<String, ThrowableAssert.ThrowingCallable> operations = new LinkedHashMap<>();
        operations.put("createDatabase", () -> metastore.createDatabase(null));
        operations.put("dropDatabase", () -> metastore.dropDatabase(SCHEMA, true));
        operations.put("renameDatabase", () -> metastore.renameDatabase(SCHEMA, "sales2"));
        operations.put("setDatabaseOwner", () -> metastore.setDatabaseOwner(SCHEMA, null));
        operations.put("createTable", () -> metastore.createTable(null, null));
        operations.put("dropTable", () -> metastore.dropTable(SCHEMA, TABLE, true));
        operations.put("replaceTable", () -> metastore.replaceTable(SCHEMA, TABLE, null, null, Map.of()));
        operations.put("renameTable", () -> metastore.renameTable(SCHEMA, TABLE, SCHEMA, "orders2"));
        operations.put("commentTable", () -> metastore.commentTable(SCHEMA, TABLE, Optional.of("comment")));
        operations.put("setTableOwner", () -> metastore.setTableOwner(SCHEMA, TABLE, null));
        operations.put("commentColumn", () -> metastore.commentColumn(SCHEMA, TABLE, "id", Optional.of("comment")));
        operations.put("addColumn", () -> metastore.addColumn(SCHEMA, TABLE, "total", HiveType.HIVE_LONG, "comment"));
        operations.put("renameColumn", () -> metastore.renameColumn(SCHEMA, TABLE, "id", "key"));
        operations.put("dropColumn", () -> metastore.dropColumn(SCHEMA, TABLE, "id"));
        operations.put("addPartitions", () -> metastore.addPartitions(SCHEMA, TABLE, List.of()));
        operations.put("dropPartition", () -> metastore.dropPartition(SCHEMA, TABLE, List.of("emea"), true));
        operations.put("alterPartition", () -> metastore.alterPartition(SCHEMA, TABLE, null));
        operations.put("updateTableStatistics", () -> metastore.updateTableStatistics(SCHEMA, TABLE, OptionalLong.empty(), StatisticsUpdateMode.OVERWRITE_ALL, null));
        operations.put("updatePartitionStatistics", () -> metastore.updatePartitionStatistics(null, StatisticsUpdateMode.OVERWRITE_ALL, Map.of()));
        operations.put("createRole", () -> metastore.createRole("analyst", "admin"));
        operations.put("dropRole", () -> metastore.dropRole("analyst"));
        operations.put("grantRoles", () -> metastore.grantRoles(Set.of("analyst"), Set.of(), false, null));
        operations.put("revokeRoles", () -> metastore.revokeRoles(Set.of("analyst"), Set.of(), false, null));
        operations.put("grantTablePrivileges", () -> metastore.grantTablePrivileges(SCHEMA, TABLE, "owner", null, null, Set.of(), false));
        operations.put("revokeTablePrivileges", () -> metastore.revokeTablePrivileges(SCHEMA, TABLE, "owner", null, null, Set.of(), false));
        operations.put("checkSupportsTransactions", metastore::checkSupportsTransactions);
        operations.put("openTransaction", () -> metastore.openTransaction(null));
        operations.put("commitTransaction", () -> metastore.commitTransaction(1));
        operations.put("abortTransaction", () -> metastore.abortTransaction(1));
        operations.put("sendTransactionHeartbeat", () -> metastore.sendTransactionHeartbeat(1));
        operations.put("acquireSharedReadLock", () -> metastore.acquireSharedReadLock(null, "query", 1, List.of(), List.of()));
        operations.put("getValidWriteIds", () -> metastore.getValidWriteIds(List.of(), 1));
        operations.put("allocateWriteId", () -> metastore.allocateWriteId(SCHEMA, TABLE, 1));
        operations.put("acquireTableWriteLock", () -> metastore.acquireTableWriteLock(null, "query", 1, SCHEMA, TABLE, AcidOperation.INSERT, false));
        operations.put("updateTableWriteId", () -> metastore.updateTableWriteId(SCHEMA, TABLE, 1, 1, OptionalLong.empty()));
        operations.put("addDynamicPartitions", () -> metastore.addDynamicPartitions(SCHEMA, TABLE, List.of(), 1, 1, AcidOperation.INSERT));
        operations.put("alterTransactionalTable", () -> metastore.alterTransactionalTable(null, 1, 1, null));
        operations.put("createFunction", () -> metastore.createFunction(SCHEMA, "f", null));
        operations.put("replaceFunction", () -> metastore.replaceFunction(SCHEMA, "f", null));
        operations.put("dropFunction", () -> metastore.dropFunction(SCHEMA, "f", "token"));

        assertThat(operations).hasSize(40);
        operations.forEach((name, operation) -> assertThatThrownBy(operation)
                .as(name)
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertErrorCode(thrown, NOT_SUPPORTED))
                .hasMessageStartingWith("This BigLake metastore supports read operations only, not "));
    }

    @Test
    void answersFunctionLookupsWithoutFailing()
    {
        assertThat(metastore.functionExists(SCHEMA, "f", "token")).isFalse();
        assertThat(metastore.getAllFunctions(SCHEMA)).isEmpty();
        assertThat(metastore.getFunctions(SCHEMA, "f")).isEmpty();
        assertThat(metastore.getTableNamesWithParameters(SCHEMA, "k", Set.of("v"))).isEmpty();
    }

    @Test
    void answersRoleAndPrivilegeLookupsWithoutFailing()
    {
        assertThat(metastore.listRoles()).isEmpty();
        assertThat(metastore.listRoleGrants(new HivePrincipal(USER, "alice"))).isEmpty();
        assertThat(metastore.listTablePrivileges(SCHEMA, TABLE, Optional.empty(), Optional.empty())).isEmpty();
    }

    @Test
    void answersStatisticsLookupsWithoutFailing()
    {
        assertThat(metastore.getTableColumnStatistics(SCHEMA, TABLE, Set.of("id"))).isEmpty();
        assertThat(metastore.getPartitionColumnStatistics(SCHEMA, TABLE, Set.of("region=emea"), Set.of("id"))).isEmpty();
        assertThat(metastore.useSparkTableStatistics()).isFalse();
    }

    private Table partitionedOrders()
    {
        bigLake.add(table(SCHEMA, TABLE, "", List.of(field("id", "bigint")), List.of(field("region", "string"))));
        return metastore.getTable(SCHEMA, TABLE).orElseThrow();
    }

    private List<String> partitionNames(TupleDomain<String> filter)
    {
        return metastore.getPartitionNamesByFilter(SCHEMA, TABLE, List.of("region"), filter).orElseThrow();
    }

    private static void assertErrorCode(Throwable thrown, ErrorCodeSupplier expected)
    {
        assertThat(((TrinoException) thrown).getErrorCode()).isEqualTo(expected.toErrorCode());
    }
}
