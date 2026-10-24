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

import com.google.api.gax.retrying.RetrySettings;
import com.google.cloud.biglake.hive.v1.HiveDatabase;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceSettings;
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

import static io.trino.metastore.biglake.TestingBigLakeObjects.CATALOG;
import static io.trino.metastore.biglake.TestingBigLakeObjects.PROJECT;
import static io.trino.metastore.biglake.TestingBigLakeObjects.database;
import static io.trino.metastore.biglake.TestingBigLakeObjects.field;
import static io.trino.metastore.biglake.TestingBigLakeObjects.table;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Runs {@link GrpcBigLakeClient} against a real gRPC server in the same process. The server is
 * {@link TestingHiveMetastoreService}, a hand-written service, so the client code, the generated
 * request and response classes, and the error handling of the client library all run for real.
 */
class TestGrpcBigLakeClient
{
    private static final String CATALOG_NAME = "projects/" + PROJECT + "/catalogs/" + CATALOG;
    private static final String SALES = CATALOG_NAME + "/databases/sales";

    private TestingHiveMetastoreService service;
    private TestingBigLakeServer server;
    private HiveMetastoreServiceClient serviceClient;
    private GrpcBigLakeClient client;

    @BeforeEach
    void setUp()
    {
        service = new TestingHiveMetastoreService();
        server = new TestingBigLakeServer(service);
        serviceClient = server.createClient(Duration.ofSeconds(30));
        client = new GrpcBigLakeClient(serviceClient, PROJECT, CATALOG);
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

        assertThat(client.listTables("sales")).extracting(TestingBigLakeObjects::tableName)
                .containsExactly("orders", "returns");
        assertThat(service.requests()).containsExactly("listHiveTables " + SALES);
    }

    @Test
    void readsEveryPageOfATableListing()
    {
        service.add(database("sales"), table("sales", "a", "", List.of(), List.of()))
                .add(database("sales"), table("sales", "b", "", List.of(), List.of()))
                .withPageSize(1);

        assertThat(client.listTables("sales")).extracting(TestingBigLakeObjects::tableName)
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
                table -> assertThat(TestingBigLakeObjects.tableName(table)).isEqualTo("orders"));
        assertThat(service.requests()).containsExactly("getHiveTable " + SALES + "/tables/orders");
    }

    @Test
    void reportsAMissingTableAsEmpty()
    {
        service.add(database("sales"));

        assertThat(client.getTable("sales", "missing")).isEmpty();
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
        });
    }

    @Test
    void namesTheReasonThatTheServiceGave()
    {
        service.failWith(Status.INTERNAL.withDescription("Error reading table"));

        assertThatThrownBy(() -> client.getTable("sales", "orders"))
                .hasMessageContaining("reading table sales.orders")
                .hasMessageContaining("Error reading table");
    }

    @Test
    void doesNotRetryAFailedCall()
    {
        service.failWith(Status.UNAVAILABLE);

        assertThatThrownBy(client::listDatabases).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.getDatabase("sales")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.listTables("sales")).isInstanceOf(TrinoException.class);
        assertThatThrownBy(() -> client.getTable("sales", "orders")).isInstanceOf(TrinoException.class);

        assertThat(service.requests()).hasSize(4);
    }

    // Settings

    @Test
    void appliesTheRequestTimeoutWithoutRetriesToEveryCall()
            throws IOException
    {
        HiveMetastoreServiceSettings settings = GrpcBigLakeClient.serviceSettings(Duration.ofSeconds(7)).build();

        List<RetrySettings> retrySettings = List.of(
                settings.listHiveDatabasesSettings().getRetrySettings(),
                settings.getHiveDatabaseSettings().getRetrySettings(),
                settings.listHiveTablesSettings().getRetrySettings(),
                settings.getHiveTableSettings().getRetrySettings());
        assertThat(retrySettings).allSatisfy(retry -> {
            assertThat(retry.getMaxAttempts()).isEqualTo(1);
            assertThat(retry.getTotalTimeoutDuration()).isEqualTo(Duration.ofSeconds(7));
        });
    }

    @Test
    void identifiesItselfAsTrinoAndCallsTheDefaultEndpoint()
            throws IOException
    {
        HiveMetastoreServiceSettings settings = GrpcBigLakeClient.serviceSettings(Duration.ofSeconds(7)).build();

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
