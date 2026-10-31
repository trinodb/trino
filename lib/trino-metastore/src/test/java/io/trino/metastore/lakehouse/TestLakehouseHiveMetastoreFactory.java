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

import com.google.cloud.biglake.hive.v1.HiveMetastoreServiceClient;
import com.google.cloud.biglake.hive.v1.stub.HiveMetastoreServiceStub;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.metastore.HiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.metastore.lakehouse.TestingLakehouseObjects.CATALOG;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.PROJECT;
import static io.trino.metastore.lakehouse.TestingLakehouseObjects.database;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestLakehouseHiveMetastoreFactory
{
    private final Tracer tracer = OpenTelemetry.noop().getTracer("test");

    private TestingLakehouseServer server;
    private final List<HiveMetastoreServiceClient> createdClients = new ArrayList<>();

    @BeforeEach
    void setUp()
    {
        server = new TestingLakehouseServer(new TestingHiveMetastoreService().add(database("sales")));
    }

    @AfterEach
    void tearDown()
    {
        server.close();
    }

    @Test
    void doesNotImpersonate()
    {
        assertThat(factory().isImpersonationEnabled()).isFalse();
    }

    @Test
    void sharesOneMetastoreAmongAllIdentities()
    {
        LakehouseHiveMetastoreFactory factory = factory();

        HiveMetastore anonymous = factory.createMetastore(Optional.empty());
        HiveMetastore alice = factory.createMetastore(Optional.of(ConnectorIdentity.ofUser("alice")));

        assertThat(alice).isSameAs(anonymous);
    }

    @Test
    void createsTheClientOnFirstUseAndOnlyOnce()
    {
        LakehouseHiveMetastoreFactory factory = factory();
        HiveMetastore metastore = factory.createMetastore(Optional.empty());
        assertThat(createdClients).isEmpty();

        assertThat(metastore.getAllDatabases()).containsExactly("sales");
        assertThat(metastore.getAllDatabases()).containsExactly("sales");

        assertThat(createdClients).hasSize(1);
    }

    @Test
    void closesTheClientItCreated()
    {
        LakehouseHiveMetastoreFactory factory = factory();
        factory.createMetastore(Optional.empty()).getAllDatabases();
        HiveMetastoreServiceClient created = createdClients.getFirst();
        assertThat(created.isShutdown()).isFalse();

        factory.close();

        assertThat(created.isShutdown()).isTrue();
    }

    @Test
    void ignoresASecondClose()
    {
        LakehouseHiveMetastoreFactory factory = factory();
        factory.createMetastore(Optional.empty()).getAllDatabases();

        factory.close();
        factory.close();

        assertThat(createdClients).hasSize(1);
    }

    @Test
    void closesNothingWhenNoClientWasCreated()
    {
        LakehouseHiveMetastoreFactory factory = factory();

        factory.close();

        assertThat(createdClients).isEmpty();
    }

    @Test
    void reportsAFailureToCloseTheClient()
    {
        HiveMetastoreServiceClient failing = HiveMetastoreServiceClient.create(new FailingToCloseStub());
        LakehouseHiveMetastoreFactory factory = new LakehouseHiveMetastoreFactory(PROJECT, CATALOG, tracer, () -> failing);
        // The call itself fails, because the stub answers nothing, but it creates the client
        assertThatThrownBy(() -> factory.createMetastore(Optional.empty()).getAllDatabases())
                .isInstanceOf(RuntimeException.class);

        assertThatThrownBy(factory::close)
                .isInstanceOfSatisfying(TrinoException.class, thrown -> assertThat(thrown.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode()))
                .hasMessage("Failed to close the Lakehouse client");
    }

    private LakehouseHiveMetastoreFactory factory()
    {
        return new LakehouseHiveMetastoreFactory(PROJECT, CATALOG, tracer, () -> {
            HiveMetastoreServiceClient created = server.createClient(Duration.ofSeconds(30));
            createdClients.add(created);
            return created;
        });
    }

    private static final class FailingToCloseStub
            extends HiveMetastoreServiceStub
    {
        @Override
        public void close()
        {
            throw new IllegalStateException("close failed");
        }

        @Override
        public void shutdown() {}

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public void shutdownNow() {}

        @Override
        public boolean awaitTermination(long duration, TimeUnit unit)
        {
            return true;
        }
    }
}
