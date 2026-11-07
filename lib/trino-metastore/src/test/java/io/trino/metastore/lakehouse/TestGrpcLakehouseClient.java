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
package io.trino.metastore.lakehouse;

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceSettings;
import com.google.cloud.biglake.hive.v1.HiveTable;
import com.google.cloud.biglake.hive.v1.Partition;
import io.grpc.Status;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import org.assertj.core.api.ThrowableAssert.ThrowingCallable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.lakehouse.TestingLakehouseObjects.CATALOG;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PROJECT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.database;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.field;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.partition;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.table;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs {@link GrpcLakehouseClient} against a real gRPC server in the same process. The server is
 * {@link TestingHiveMetastoreService}, a hand-written service, so the client code, the generated
 * request and response classes, and the error handling of the client library all run for real.
 */
class TestGrpcLakehouseClient
{
    private static final String CATALOG_NAME = "projects/" + PROJECT + "/catalogs/" + CATALOG;
    private static final String SALES = CATALOG_NAME + "/databases/sales";

    private TestingHiveMetastoreService service;
    private TestingLakehouseServer server;
    private HiveMetastoreServiceClient serviceClient;
    private GrpcLakehouseClient client;

    @BeforeEach
    void setUp()
    {
        service = new TestingHiveMetastoreService();
        server = new TestingLakehouseServer(service);
        serviceClient = server.createClient(Duration.ofSeconds(30));
        client = new GrpcLakehouseClient(serviceClient, PROJECT, CATALOG);
    }

    @AfterEach
    void tearDown()
    {
        client.close();
        server.close();
    }

    // Databases

    @Test
    void listsTheDatabasesOfTheCatalog()
    {
        service.add(database("sales")).add(database("logs"));

        assertThat(client.listDatabases()).extracting(HiveDatabase::getName)
                .containsExactly(SALES, CATALOG_NAME + "/databases/logs");
        assertThat(service.requests()).containsExactly("listHiveDatabases " + CATALOG_NAME);
    }

    @Test
    void readsEveryPageOfADatabaseListing()
    {
        service.add(database("a")).add(database("b")).add(database("c")).withPageSize(1);

        assertThat(client.listDatabases()).extracting(HiveDatabase::getName)
                .containsExactly(
                        CATALOG_NAME + "/databases/a",
                        CATALOG_NAME + "/databases/b",
                        CATALOG_NAME + "/databases/c");
        assertThat(service.requests()).hasSize(3);
    }

    @Test
    void failsOnAnUnknownCatalogInsteadOfReportingNoDatabases()
    {
        service.withoutCatalog();

        assertFailure(client::listDatabases, GENERIC_INTERNAL_ERROR, "listing databases");
        assertFailure(() -> client.getDatabase("sales"), GENERIC_INTERNAL_ERROR, "reading database sales");
    }

    @Test
    void failsOnAnUnknownProjectInsteadOfReportingNoDatabases()
    {
        service.withoutProject();

        assertFailure(client::listDatabases, GENERIC_INTERNAL_ERROR, "listing databases");
    }

    @Test
    void returnsADatabaseByItsResourceName()
    {
        service.add(database("sales"));

        assertThat(client.getDatabase("sales")).hasValueSatisfying(
                database -> assertThat(database.getName()).isEqualTo(SALES));
        assertThat(service.requests()).containsExactly("getHiveDatabase " + SALES);
    }

    @Test
    void reportsAMissingDatabaseAsEmpty()
    {
        assertThat(client.getDatabase("missing")).isEmpty();
    }

    // Tables

    @Test
    void listsTheTablesOfADatabase()
    {
        service.add(database("sales"), table("sales", "orders", "", List.of(field("id", "bigint")), List.of()))
                .add(database("sales"), table("sales", "returns", "", List.of(field("id", "bigint")), List.of()));

        assertThat(client.listTables("sales")).extracting(TestingLakehouseObjects::tableName)
                .containsExactly("orders", "returns");
        assertThat(service.requests()).containsExactly("listHiveTables " + SALES);
    }

    @Test
    void readsEveryPageOfATableListing()
    {
        service.add(database("sales"), table("sales", "a", "", List.of(), List.of()))
                .add(database("sales"), table("sales", "b", "", List.of(), List.of()))
                .withPageSize(1);

        assertThat(client.listTables("sales")).extracting(TestingLakehouseObjects::tableName)
                .containsExactly("a", "b");
        assertThat(service.requests()).hasSize(2);
    }

    @Test
    void reportsTheTablesOfAMissingDatabaseAsEmpty()
    {
        assertThat(client.listTables("missing")).isEmpty();
    }

    @Test
    void reportsATableOfAMissingDatabaseAsEmpty()
    {
        assertThat(client.getTable("missing", "orders")).isEmpty();
    }

    @Test
    void returnsATableByItsResourceName()
    {
        service.add(database("sales"), table("sales", "orders", "", List.of(field("id", "bigint")), List.of()));

        assertThat(client.getTable("sales", "orders")).hasValueSatisfying(
                table -> assertThat(TestingLakehouseObjects.tableName(table)).isEqualTo("orders"));
        assertThat(service.requests()).containsExactly("getHiveTable " + SALES + "/tables/orders");
    }

    @Test
    void reportsAMissingTableAsEmpty()
    {
        service.add(database("sales"));

        assertThat(client.getTable("sales", "missing")).isEmpty();
    }

    // Partitions

    @Test
    void joinsTheResponsesOfAPartitionStreamInOrder()
    {
        HiveTable orders = table("sales", "orders", "", List.of(field("id", "bigint")), List.of(field("region", "string")));
        service.add(database("sales"), orders).withPartitionsPerResponse(2);
        for (String region : List.of("a", "b", "c", "d", "e")) {
            service.add(orders, partition("orders", List.of(region)));
        }

        Optional<List<Partition>> partitions = client.listPartitions("sales", "orders", "");

        assertThat(partitions).isPresent();
        assertThat(partitions.get()).extracting(partition -> partition.getValues(0))
                .containsExactly("a", "b", "c", "d", "e");
        assertThat(service.requests()).containsExactly("listPartitions " + SALES + "/tables/orders");
    }

    @Test
    void sendsTheFilterToTheService()
    {
        HiveTable orders = table("sales", "orders", "", List.of(field("id", "bigint")), List.of(field("region", "string")));
        service.add(database("sales"), orders);

        client.listPartitions("sales", "orders", "region='emea'");
        client.listPartitions("sales", "orders", "");

        assertThat(service.partitionFilters()).containsExactly("region='emea'", "");
    }

    @Test
    void reportsThePartitionsOfAMissingTableAsAnAbsentResult()
    {
        service.add(database("sales"));

        assertThat(client.listPartitions("sales", "missing", "")).isEmpty();
    }

    @Test
    void reportsThePartitionsOfAMissingDatabaseAsAnAbsentResult()
    {
        assertThat(client.listPartitions("missing", "orders", "")).isEmpty();
    }

    // Failures

    @Test
    void translatesEveryFailureInsteadOfHidingItAsAnEmptyResult()
    {
        Map<Status, ErrorCodeSupplier> expected = Map.of(
                Status.PERMISSION_DENIED, PERMISSION_DENIED,
                Status.UNAUTHENTICATED, PERMISSION_DENIED,
                Status.INVALID_ARGUMENT, INVALID_ARGUMENTS,
                Status.OUT_OF_RANGE, INVALID_ARGUMENTS,
                Status.UNAVAILABLE, GENERIC_INTERNAL_ERROR,
                Status.INTERNAL, GENERIC_INTERNAL_ERROR);

        expected.forEach((status, errorCode) -> {
            service.failWith(status);

            assertFailure(client::listDatabases, errorCode, "listing databases");
            assertFailure(() -> client.getDatabase("sales"), errorCode, "reading database sales");
            assertFailure(() -> client.listTables("sales"), errorCode, "listing tables of sales");
            assertFailure(() -> client.getTable("sales", "orders"), errorCode, "reading table sales.orders");
            assertFailure(() -> client.listPartitions("sales", "orders", ""), errorCode, "listing partitions of sales.orders");
        });
    }

    @Test
    void namesTheReasonThatTheServiceGave()
    {
        service.failWith(Status.INTERNAL.withDescription("Error reading partitions"));

        assertThatThrownBy(() -> client.listPartitions("sales", "orders", ""))
                .hasMessageContaining("listing partitions of sales.orders")
                .hasMessageContaining("Error reading partitions");
    }

    @Test
    void doesNotRetryAFailedCall()
    {
        service.failWith(Status.UNAVAILABLE);

        assertThatThrownBy(client::listDatabases).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.getDatabase("sales")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.listTables("sales")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.getTable("sales", "orders")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.listPartitions("sales", "orders", "")).isInstanceOf(TrinoException.class);

        assertThat(service.requests()).hasSize(5);
    }

    // Settings

    @Test
    void appliesTheRequestTimeoutWithoutRetriesToEveryCall()
            throws IOException
    {
        HiveMetastoreServiceSettings settings = GrpcLakehouseClient.serviceSettings(Duration.ofSeconds(7)).build();

        List<RetrySettings> retrySettings = List.of(
                settings.listHiveDatabasesSettings().getRetrySettings(),
                settings.getHiveDatabaseSettings().getRetrySettings(),
                settings.listHiveTablesSettings().getRetrySettings(),
                settings.getHiveTableSettings().getRetrySettings(),
                settings.listPartitionsSettings().getRetrySettings());
        assertThat(retrySettings).allSatisfy(retry -> {
            assertThat(retry.getMaxAttempts()).isEqualTo(1);
            assertThat(retry.getTotalTimeoutDuration()).isEqualTo(Duration.ofSeconds(7));
        });
    }

    @Test
    void identifiesItselfAsTrinoAndCallsTheDefaultEndpoint()
            throws IOException
    {
        HiveMetastoreServiceSettings settings = GrpcLakehouseClient.serviceSettings(Duration.ofSeconds(7)).build();

        assertThat(settings.getHeaderProvider().getHeaders()).containsEntry("user-agent", "Trino");
        assertThat(settings.getEndpoint()).isEqualTo("biglake.googleapis.com:443");
    }

    // Lifecycle

    @Test
    void closesTheServiceClient()
    {
        assertThat(serviceClient.isShutdown()).isFalse();

        client.close();

        assertThat(serviceClient.isShutdown()).isTrue();
    }

    private static void assertFailure(ThrowingCallable call, ErrorCodeSupplier expected, String operation)
    {
        assertThatThrownBy(call)
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertThat(thrown.getErrorCode()).isEqualTo(expected.toErrorCode()))
                .hasMessageContaining(operation);
    }
}
