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
package io.trino.plugin.kudu;

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import jakarta.annotation.PreDestroy;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import static io.airlift.concurrent.Threads.threadsNamed;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KuduScannerAliveKeeper
{
    private final Logger logger = Logger.get(KuduScannerAliveKeeper.class);
    private final ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4, threadsNamed("kudu-scanner-keepalive-%s"));
    private final Duration scannerKeepAliveInterval;

    @Inject
    public KuduScannerAliveKeeper(KuduClientConfig kuduClientConfig)
    {
        this.scannerKeepAliveInterval = requireNonNull(kuduClientConfig, "kuduClientConfig is null").getScannerKeepAliveInterval();
    }

    public ScheduledFuture<?> add(KuduScanner scanner)
    {
        long scannerKeepAliveIntervalMillis = scannerKeepAliveInterval.toMillis();
        return executorService.scheduleAtFixedRate(() -> {
            try {
                scanner.keepAlive();
            }
            catch (KuduException e) {
                // a KuduException thrown by keepAlive should not be taken as indication that the scan has failed
                logger.warn(e, "Failed to keep the current scanner alive");
            }
        }, scannerKeepAliveIntervalMillis, scannerKeepAliveIntervalMillis, MILLISECONDS);
    }

    @PreDestroy
    public void shutdown()
    {
        executorService.shutdownNow();
    }
}
