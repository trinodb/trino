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
package io.trino.plugin.exchange.filesystem.s3;

import io.trino.plugin.exchange.filesystem.AbstractTestExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.filesystem.TestExchangeManagerContext;
import io.trino.plugin.exchange.filesystem.containers.FlociStorage;
import io.trino.spi.exchange.ExchangeManager;
import org.junit.jupiter.api.AfterAll;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.exchange.filesystem.s3.ExchangeS3Config.S3SseType.S3;
import static java.util.UUID.randomUUID;

public class TestS3FileSystemExchangeManagerSseS3
        extends AbstractTestExchangeManager
{
    private FlociStorage storage;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        storage = new FlociStorage("test-exchange-spooling-" + randomUUID(), S3);
        storage.start();

        return new FileSystemExchangeManagerFactory().create(
                storage.getExchangeManagerProperties(),
                new TestExchangeManagerContext());
    }

    @AfterAll
    public void cleanUp()
            throws Exception
    {
        closeAll(storage);
        storage = null;
    }
}
