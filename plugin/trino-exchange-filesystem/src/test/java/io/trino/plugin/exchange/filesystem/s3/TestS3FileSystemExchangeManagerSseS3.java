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
import io.trino.plugin.exchange.filesystem.containers.MinioStorage;
import io.trino.spi.exchange.ExchangeManager;
import org.junit.jupiter.api.AfterAll;

import static io.airlift.testing.Closeables.closeAll;
import static io.trino.plugin.exchange.filesystem.containers.MinioStorage.getExchangeManagerPropertiesWithSseS3;
import static java.util.UUID.randomUUID;

public class TestS3FileSystemExchangeManagerSseS3
        extends AbstractTestExchangeManager
{
    private MinioStorage minioStorage;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomUUID(), true);
        minioStorage.start();

        return new FileSystemExchangeManagerFactory().create(
                getExchangeManagerPropertiesWithSseS3(minioStorage),
                new TestExchangeManagerContext());
    }

    @AfterAll
    public void cleanUp()
            throws Exception
    {
        closeAll(minioStorage);
    }
}
