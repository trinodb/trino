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
package io.trino.plugin.raptor.legacy.backup;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import io.trino.spi.NodeManager;
import io.trino.testing.TestingNodeManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.Supplier;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static com.google.inject.util.Modules.override;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static java.nio.file.Files.createTempDirectory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Execution(ExecutionMode.SAME_THREAD)
public class TestHttpBackupStore
        extends AbstractTestBackupStore<BackupStore>
{
    private LifeCycleManager lifeCycleManager;

    @BeforeEach
    public void setup()
            throws IOException
    {
        temporary = createTempDirectory(null);

        Map<String, String> properties = ImmutableMap.of("backup.http.uri", "http://localhost:8080");

        Bootstrap app = new Bootstrap(
                new TestingNodeModule(),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                binder -> jaxrsBinder(binder).bind(TestingHttpBackupResource.class),
                binder -> binder.bind(NodeManager.class).toInstance(new TestingNodeManager()),
                override(new HttpBackupModule()).with(new TestingModule()));

        Injector injector = app
                .setRequiredConfigurationProperties(properties)
                .doNotInitializeLogging()
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        store = injector.getInstance(BackupStore.class);
    }

    @AfterEach
    public void teardown()
            throws IOException
    {
        deleteRecursively(temporary, ALLOW_INSECURE);
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }

    private static class TestingModule
            implements Module
    {
        @Override
        public void configure(Binder binder) {}

        @Provides
        @Singleton
        @ForHttpBackup
        public Supplier<URI> createBackupUriSupplier(HttpServerInfo serverInfo)
        {
            return serverInfo::getHttpUri;
        }
    }
}
