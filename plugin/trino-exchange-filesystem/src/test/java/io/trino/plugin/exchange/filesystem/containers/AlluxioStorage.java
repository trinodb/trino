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
package io.trino.plugin.exchange.filesystem.containers;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.containers.Alluxio;

import java.util.Map;

public class AlluxioStorage
        implements AutoCloseable
{
    private final Alluxio alluxio = new Alluxio();

    public void start()
    {
        alluxio.start();
    }

    @Override
    public void close()
            throws Exception
    {
        alluxio.close();
    }

    public static Map<String, String> getExchangeManagerProperties()
    {
        return ImmutableMap.<String, String>builder()
                .put("exchange.base-directories", "alluxio:///trino-exchange")
                // to trigger file split in some tests
                .put("exchange.sink-max-file-size", "16MB")
                // create more granular source handles given the fault-tolerant execution target task input size is set to lower value for testing
                .put("exchange.source-handle-target-data-size", "1MB")
                .buildOrThrow();
    }
}
