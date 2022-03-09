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
package io.trino.plugin.exchange.s3;

import io.trino.plugin.exchange.AbstractTestExchangeManager;
import io.trino.plugin.exchange.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.containers.MinioStorage;
import io.trino.spi.exchange.ExchangeManager;
import org.testng.annotations.AfterClass;

import static io.trino.plugin.exchange.containers.MinioStorage.getExchangeManagerProperties;
import static java.util.UUID.randomUUID;

public class TestS3FileSystemExchangeManager
        extends AbstractTestExchangeManager
{
    private MinioStorage minioStorage;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomUUID());
        minioStorage.start();

        return new FileSystemExchangeManagerFactory().create(getExchangeManagerProperties(minioStorage));
    }

    @Override
    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        super.destroy();
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
