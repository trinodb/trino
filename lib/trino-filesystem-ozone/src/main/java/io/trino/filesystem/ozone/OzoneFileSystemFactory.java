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
package io.trino.filesystem.ozone;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.inject.Inject;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.security.ConnectorIdentity;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.client.ObjectStore;
import org.apache.hadoop.ozone.client.OzoneClient;
import org.apache.hadoop.ozone.client.OzoneClientFactory;

import java.io.IOException;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class OzoneFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final ListeningExecutorService executorService;

    @Inject
    public OzoneFileSystemFactory(OzoneFileSystemConfig config)
    {
        requireNonNull(config.getWriteBlockSize());
        this.executorService = listeningDecorator(newCachedThreadPool(daemonThreadsNamed("trino-filesystem-gcs-%S")));
    }

    @PreDestroy
    public void stop()
    {
        executorService.shutdownNow();
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        OzoneConfiguration conf = new OzoneConfiguration();
        try {
            OzoneClient ozoneClient = OzoneClientFactory.getRpcClient("127.0.0.1", 9862, conf);
            ObjectStore objectStore = ozoneClient.getObjectStore();
            return new OzoneFileSystem(objectStore);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
