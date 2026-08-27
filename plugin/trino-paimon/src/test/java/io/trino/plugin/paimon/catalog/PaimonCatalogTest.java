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
package io.trino.plugin.paimon.catalog;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.TestingConnectorSession;
import org.apache.paimon.PagedList;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogLoader;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.view.View;
import org.apache.paimon.view.ViewImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.paimon.options.CatalogOptions.WAREHOUSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PaimonCatalogTest
{
    @TempDir
    Path root;

    @Test
    public void testCatalogLoaderRequiresSessionInitialization()
    {
        PaimonCatalog catalog = catalog();

        assertThatThrownBy(catalog::catalogLoader)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon catalog has not been initialized for a Trino session");
    }

    @Test
    public void testCatalogRejectsNullDependencies()
    {
        assertThatThrownBy(() -> new PaimonCatalog(null, new LocalFileSystemFactory(root)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("options is null");
        assertThatThrownBy(() -> new PaimonCatalog(new Options(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("paimonFileSystemFactory is null");
        assertThatThrownBy(() -> new PaimonCatalog(new Options(), new LocalFileSystemFactory(root), 0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("sessionCatalogCacheMaximumSize must be greater than zero: 0");
        assertThatThrownBy(() -> catalog().initSession(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("connectorSession is null");
    }

    @Test
    public void testCatalogLoaderDelegatesToInitializedCatalog()
            throws Exception
    {
        PaimonCatalog catalog = catalog();
        catalog.initSession(TestingConnectorSession.SESSION);
        catalog.createDatabase("test_schema", false, Map.of());

        CatalogLoader catalogLoader = catalog.catalogLoader();

        assertThat(catalogLoader).isNotNull();
        try (Catalog reloaded = catalogLoader.load()) {
            assertThat(reloaded.listDatabases()).contains("test_schema");
        }
    }

    @Test
    public void testCatalogDoesNotInjectNoHadoopFormatProviders()
    {
        PaimonCatalog catalog = catalog();
        catalog.initSession(TestingConnectorSession.SESSION);

        assertThat(catalog.options().keySet())
                .noneMatch(key -> key.startsWith("table.runtime." + "file.format."));
    }

    @Test
    public void testCatalogInitReportsWarehouseAccessFailureWithoutHadoopFallback()
    {
        Options options = new Options();
        options.set(WAREHOUSE, "s3://bucket/warehouse");
        PaimonCatalog catalog = new PaimonCatalog(options, _ -> failingFileSystem());

        assertThatThrownBy(() -> catalog.initSession(TestingConnectorSession.SESSION))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to access Paimon warehouse 's3://bucket/warehouse' with Trino file system")
                .hasMessageNotContaining("Hadoop configuration is not available")
                .hasRootCauseInstanceOf(IOException.class)
                .hasRootCauseMessage("simulated S3 probe failure");
    }

    @Test
    public void testLocalCatalogViewOperationsRemainUnsupported()
            throws Exception
    {
        PaimonCatalog catalog = catalog();
        catalog.initSession(TestingConnectorSession.SESSION);
        Identifier viewName = new Identifier("view_db", "source_view");
        View view = view(viewName, "SELECT id FROM source_table", "initial comment");

        catalog.createDatabase(viewName.getDatabaseName(), false, Map.of());

        assertThatThrownBy(() -> catalog.createView(viewName, view, false))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testCatalogReusesDelegateForSameIdentity()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        TestingConnectorSession alice = session("alice");

        catalog.initSession(alice);
        catalog.createDatabase("alice_db", false, Map.of());

        catalog.initSession(alice);

        assertThat(catalog.listDatabases()).contains("alice_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(1);
    }

    @Test
    public void testCatalogSeparatesDelegatesForDifferentIdentities()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        TestingConnectorSession alice = session("alice");
        TestingConnectorSession bob = session("bob");

        catalog.initSession(alice);
        catalog.createDatabase("alice_db", false, Map.of());

        catalog.initSession(bob);
        assertThat(catalog.listDatabases()).doesNotContain("alice_db");

        catalog.createDatabase("bob_db", false, Map.of());
        assertThat(catalog.listDatabases()).contains("bob_db").doesNotContain("alice_db");

        catalog.initSession(alice);
        assertThat(catalog.listDatabases()).contains("alice_db").doesNotContain("bob_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(2);
    }

    @Test
    public void testCatalogSeparatesDelegatesForDifferentExtraCredentials()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        TestingConnectorSession aliceOne = session("alice", Map.of("token", "one"));
        TestingConnectorSession aliceTwo = session("alice", Map.of("token", "two"));

        catalog.initSession(aliceOne);
        catalog.createDatabase("first_db", false, Map.of());

        catalog.initSession(aliceTwo);
        assertThat(catalog.listDatabases()).doesNotContain("first_db");

        catalog.createDatabase("second_db", false, Map.of());
        assertThat(catalog.listDatabases()).contains("second_db").doesNotContain("first_db");

        catalog.initSession(aliceOne);
        assertThat(catalog.listDatabases()).contains("first_db").doesNotContain("second_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(2);
    }

    @Test
    public void testCatalogEvictsLeastRecentlyUsedSessionCatalog()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory, 2);
        TestingConnectorSession alice = session("alice");
        TestingConnectorSession bob = session("bob");
        TestingConnectorSession carol = session("carol");

        catalog.initSession(alice);
        catalog.createDatabase("alice_db", false, Map.of());
        catalog.initSession(bob);
        catalog.createDatabase("bob_db", false, Map.of());
        catalog.initSession(alice);
        catalog.initSession(carol);
        catalog.createDatabase("carol_db", false, Map.of());

        assertThat(catalog.cachedCatalogCount()).isEqualTo(2);
        assertThat(fileSystemFactory.createCalls()).hasValue(3);

        catalog.initSession(alice);
        assertThat(catalog.listDatabases()).contains("alice_db").doesNotContain("bob_db", "carol_db");
        assertThat(fileSystemFactory.createCalls()).hasValue(3);

        catalog.initSession(bob);
        assertThat(catalog.listDatabases()).contains("bob_db").doesNotContain("alice_db", "carol_db");
        assertThat(catalog.cachedCatalogCount()).isEqualTo(2);
        assertThat(fileSystemFactory.createCalls()).hasValue(4);
    }

    @Test
    public void testCatalogDefersEvictedCatalogCloseUntilOperationCompletes()
            throws Exception
    {
        BlockingCatalog aliceCatalog = new BlockingCatalog();
        BlockingCatalog bobCatalog = new BlockingCatalog();
        PaimonCatalog catalog = new PaimonCatalog(
                new Options(),
                new LocalFileSystemFactory(root),
                1,
                session -> session.getIdentity().getUser().equals("alice") ? aliceCatalog.catalog() : bobCatalog.catalog());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> operation = executor.submit(() -> catalog.forSession(session("alice")).listDatabases());
            assertThat(aliceCatalog.awaitOperationStarted()).isTrue();

            catalog.forSession(session("bob"));
            assertThat(aliceCatalog.closeCalls()).hasValue(0);

            aliceCatalog.releaseOperation();
            operation.get(30, TimeUnit.SECONDS);
            assertThat(aliceCatalog.closeCalls()).hasValue(1);
        }
        finally {
            aliceCatalog.releaseOperation();
            catalog.close();
            executor.shutdownNow();
        }
        assertThat(bobCatalog.closeCalls()).hasValue(1);
    }

    @Test
    public void testCatalogRejectsSessionInitializationAfterClose()
            throws Exception
    {
        RecordingFileSystemFactory fileSystemFactory = new RecordingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);

        catalog.close();

        assertThatThrownBy(() -> catalog.initSession(session("alice")))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon catalog is already closed");
        setCurrentCatalog(catalog, new RecordingCatalog().catalog());
        assertThatThrownBy(catalog::listDatabases)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon catalog is already closed");
        assertThat(catalog.cachedCatalogCount()).isZero();
        assertThat(fileSystemFactory.createCalls()).hasValue(0);
    }

    @Test
    public void testCatalogCloseWinsConcurrentSessionCatalogCreation()
            throws Exception
    {
        BlockingFileSystemFactory fileSystemFactory = new BlockingFileSystemFactory(root);
        PaimonCatalog catalog = catalog(fileSystemFactory);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> initSession = executor.submit(() -> catalog.initSession(session("alice")));
            assertThat(fileSystemFactory.awaitCreateStarted()).isTrue();

            catalog.close();
            fileSystemFactory.releaseCreate();

            assertThatThrownBy(() -> initSession.get(30, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("Paimon catalog is already closed");
            assertThat(catalog.cachedCatalogCount()).isZero();
        }
        finally {
            fileSystemFactory.releaseCreate();
            executor.shutdownNow();
        }
    }

    @Test
    public void testCatalogDelegatesPaimonDefaultMethodsToCurrentCatalog()
            throws Exception
    {
        PaimonCatalog catalog = catalog();
        RecordingCatalog recordingCatalog = new RecordingCatalog();
        setCurrentCatalog(catalog, recordingCatalog.catalog());
        Identifier identifier = new Identifier("default", "test_table");

        catalog.listTablesPagedGlobally("default%", "test%", 10, "page");
        catalog.invalidateTable(identifier);
        catalog.repairCatalog();
        catalog.repairDatabase("default");
        catalog.repairTable(identifier);
        catalog.registerTable(identifier, "s3://warehouse/default.db/test_table");
        catalog.listConsumersPaged(identifier, 10, "page");
        catalog.resetConsumer(identifier, "consumer", 1L);
        catalog.rollbackSchema(identifier, 1);
        catalog.createBranch(identifier, "branch", "tag", true);
        catalog.listFunctionsPaged("default", 10, "page", "fn%");
        catalog.listFunctionsPagedGlobally("default%", "fn%", 10, "page");
        catalog.listFunctionDetailsPaged("default", 10, "page", "fn%");

        assertThat(recordingCatalog.calls()).containsExactly(
                "listTablesPagedGlobally",
                "invalidateTable",
                "repairCatalog",
                "repairDatabase",
                "repairTable",
                "registerTable",
                "listConsumersPaged",
                "resetConsumer",
                "rollbackSchema",
                "createBranch",
                "listFunctionsPaged",
                "listFunctionsPagedGlobally",
                "listFunctionDetailsPaged");
    }

    private PaimonCatalog catalog()
    {
        return catalog(new LocalFileSystemFactory(root));
    }

    private PaimonCatalog catalog(TrinoFileSystemFactory fileSystemFactory)
    {
        return catalog(fileSystemFactory, PaimonCatalog.DEFAULT_SESSION_CATALOG_CACHE_MAXIMUM_SIZE);
    }

    private PaimonCatalog catalog(TrinoFileSystemFactory fileSystemFactory, int sessionCatalogCacheMaximumSize)
    {
        Options options = new Options();
        options.set(WAREHOUSE, "local:///warehouse");
        return new PaimonCatalog(options, fileSystemFactory, sessionCatalogCacheMaximumSize);
    }

    private static View view(Identifier identifier, String query, String comment)
    {
        return new ViewImpl(
                identifier,
                List.of(DataTypes.FIELD(0, "id", DataTypes.BIGINT(), "id column")),
                query,
                Map.of("trino", query),
                comment,
                Map.of("comment", comment));
    }

    private static TestingConnectorSession session(String user)
    {
        return session(user, Map.of());
    }

    private static TestingConnectorSession session(String user, Map<String, String> extraCredentials)
    {
        return TestingConnectorSession.builder()
                .setIdentity(ConnectorIdentity.forUser(user)
                        .withExtraCredentials(extraCredentials)
                        .build())
                .build();
    }

    private static TrinoFileSystem failingFileSystem()
    {
        return (TrinoFileSystem) Proxy.newProxyInstance(
                PaimonCatalogTest.class.getClassLoader(),
                new Class<?>[] {TrinoFileSystem.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return handleObjectMethod(method.getName(), proxy, args);
                    }
                    if (method.getName().equals("newInputFile")) {
                        return failingInputFile((Location) args[0]);
                    }
                    if (method.getName().equals("directoryExists")) {
                        throw new IOException("simulated S3 probe failure");
                    }
                    throw new AssertionError("Unexpected filesystem call: " + method.getName());
                });
    }

    private static TrinoInputFile failingInputFile(Location location)
    {
        return (TrinoInputFile) Proxy.newProxyInstance(
                PaimonCatalogTest.class.getClassLoader(),
                new Class<?>[] {TrinoInputFile.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return handleObjectMethod(method.getName(), proxy, args);
                    }
                    if (method.getName().equals("exists")) {
                        throw new IOException("simulated S3 probe failure");
                    }
                    if (method.getName().equals("location")) {
                        return location;
                    }
                    throw new AssertionError("Unexpected input file call: " + method.getName());
                });
    }

    private static Object handleObjectMethod(String name, Object proxy, Object[] args)
    {
        return switch (name) {
            case "toString" -> proxy.getClass().getInterfaces()[0].getSimpleName() + " proxy";
            case "hashCode" -> System.identityHashCode(proxy);
            case "equals" -> proxy == args[0];
            default -> throw new AssertionError("Unexpected Object method: " + name);
        };
    }

    @SuppressWarnings("unchecked")
    private static void setCurrentCatalog(PaimonCatalog catalog, Catalog currentCatalog)
            throws Exception
    {
        Field currentCatalogField = PaimonCatalog.class.getDeclaredField("currentCatalog");
        currentCatalogField.setAccessible(true);
        ((ThreadLocal<Catalog>) currentCatalogField.get(catalog)).set(currentCatalog);
    }

    private static final class RecordingCatalog
    {
        private final List<String> calls = new ArrayList<>();

        private Catalog catalog()
        {
            return (Catalog) Proxy.newProxyInstance(
                    PaimonCatalogTest.class.getClassLoader(),
                    new Class<?>[] {Catalog.class},
                    (_, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return method.invoke(this, args);
                        }
                        calls.add(method.getName());
                        if (method.getReturnType() == boolean.class) {
                            return false;
                        }
                        if (method.getReturnType() == Map.class) {
                            return Map.of();
                        }
                        if (method.getReturnType() == List.class) {
                            return List.of();
                        }
                        if (method.getReturnType() == PagedList.class) {
                            return new PagedList<>(List.of(), null);
                        }
                        return null;
                    });
        }

        private List<String> calls()
        {
            return calls;
        }
    }

    private static final class BlockingCatalog
    {
        private final CountDownLatch operationStarted = new CountDownLatch(1);
        private final CountDownLatch releaseOperation = new CountDownLatch(1);
        private final AtomicInteger closeCalls = new AtomicInteger();

        private Catalog catalog()
        {
            return (Catalog) Proxy.newProxyInstance(
                    PaimonCatalogTest.class.getClassLoader(),
                    new Class<?>[] {Catalog.class},
                    (proxy, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return handleObjectMethod(method.getName(), proxy, args);
                        }
                        if (method.getName().equals("listDatabases")) {
                            operationStarted.countDown();
                            if (!releaseOperation.await(30, TimeUnit.SECONDS)) {
                                throw new IllegalStateException("Timed out waiting to release catalog operation");
                            }
                            return List.of();
                        }
                        if (method.getName().equals("close")) {
                            closeCalls.incrementAndGet();
                            return null;
                        }
                        if (method.getReturnType() == boolean.class) {
                            return false;
                        }
                        if (method.getReturnType() == Map.class) {
                            return Map.of();
                        }
                        if (method.getReturnType() == List.class) {
                            return List.of();
                        }
                        if (method.getReturnType() == PagedList.class) {
                            return new PagedList<>(List.of(), null);
                        }
                        return null;
                    });
        }

        private boolean awaitOperationStarted()
                throws InterruptedException
        {
            return operationStarted.await(30, TimeUnit.SECONDS);
        }

        private void releaseOperation()
        {
            releaseOperation.countDown();
        }

        private AtomicInteger closeCalls()
        {
            return closeCalls;
        }
    }

    private static final class RecordingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final Path root;
        private final AtomicInteger createCalls = new AtomicInteger();

        private RecordingFileSystemFactory(Path root)
        {
            this.root = root;
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            createCalls.incrementAndGet();
            Path userRoot = root.resolve(identity.getUser() + "-" + Integer.toHexString(identity.getExtraCredentials().hashCode()));
            try {
                Files.createDirectories(userRoot);
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return new LocalFileSystemFactory(userRoot).create(identity);
        }

        public AtomicInteger createCalls()
        {
            return createCalls;
        }
    }

    private static final class BlockingFileSystemFactory
            implements TrinoFileSystemFactory
    {
        private final RecordingFileSystemFactory delegate;
        private final CountDownLatch createStarted = new CountDownLatch(1);
        private final CountDownLatch releaseCreate = new CountDownLatch(1);

        private BlockingFileSystemFactory(Path root)
        {
            this.delegate = new RecordingFileSystemFactory(root);
        }

        @Override
        public TrinoFileSystem create(ConnectorIdentity identity)
        {
            createStarted.countDown();
            try {
                if (!releaseCreate.await(30, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to release catalog creation");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            return delegate.create(identity);
        }

        private boolean awaitCreateStarted()
                throws InterruptedException
        {
            return createStarted.await(30, TimeUnit.SECONDS);
        }

        private void releaseCreate()
        {
            releaseCreate.countDown();
        }
    }
}
