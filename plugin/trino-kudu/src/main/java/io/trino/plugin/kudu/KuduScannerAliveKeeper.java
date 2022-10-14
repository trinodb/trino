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

import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static io.airlift.concurrent.Threads.threadsNamed;

public class KuduScannerAliveKeeper
{
    private static final Logger logger = Logger.get(KuduScannerAliveKeeper.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, threadsNamed("kudu-scanner-keepalive-%s"));

    public static void add(KuduScanner scanner, Duration keepaliveInterval)
    {
        final long keepaliveIntervalMillis = keepaliveInterval.toMillis();
        scheduler.scheduleAtFixedRate(() -> {
            if (scanner.isClosed()) {
                // this exception will terminate the schedule
                throw new RuntimeException("no need to keepalive any longer");
            }
            try {
                scanner.keepAlive();
            }
            catch (KuduException e) {
                // a KuduException thrown by keepAlive should not be taken as indication that the scan has failed
                logger.warn("fail to send keepalive", e);
            }
        }, keepaliveIntervalMillis, keepaliveIntervalMillis, TimeUnit.MILLISECONDS);
    }
}
