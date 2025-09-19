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
package io.trino.plugin.exchange.filesystem.alluxio;

import io.trino.plugin.exchange.filesystem.AbstractTestExchangeManager;
import io.trino.plugin.exchange.filesystem.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.filesystem.TestExchangeManagerContext;
import io.trino.plugin.exchange.filesystem.containers.AlluxioStorage;
import io.trino.spi.exchange.ExchangeManager;
import org.junit.jupiter.api.AfterAll;

import static io.trino.plugin.exchange.filesystem.containers.AlluxioStorage.getExchangeManagerProperties;

public class TestAlluxioFileSystemExchangeManager
        extends AbstractTestExchangeManager
{
    private AlluxioStorage alluxioStorage;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        this.alluxioStorage = new AlluxioStorage();
        alluxioStorage.start();

        return new FileSystemExchangeManagerFactory().create(
                getExchangeManagerProperties(),
                new TestExchangeManagerContext());
    }

    @Override
    @AfterAll
    public void destroy()
            throws Exception
    {
        super.destroy();
        if (alluxioStorage != null) {
            alluxioStorage.close();
            alluxioStorage = null;
        }
    }
}
