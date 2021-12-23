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

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.exchange.FileSystemExchangeManagerFactory;
import io.trino.plugin.exchange.containers.MinioStorage;
import io.trino.spi.exchange.ExchangeManager;
import io.trino.testing.AbstractTestExchangeManager;
import org.testng.annotations.AfterClass;

import java.util.Map;

import static io.trino.testing.sql.TestTable.randomTableSuffix;

public class TestS3FileSystemExchangeManager
        extends AbstractTestExchangeManager
{
    private MinioStorage minioStorage;

    @Override
    protected ExchangeManager createExchangeManager()
    {
        this.minioStorage = new MinioStorage("test-exchange-spooling-" + randomTableSuffix());
        minioStorage.start();

        Map<String, String> exchangeManagerProperties = new ImmutableMap.Builder<String, String>()
                .put("exchange.base-directory", "s3n://" + minioStorage.getBucketName())
                .put("exchange.s3.aws-access-key", MinioStorage.ACCESS_KEY)
                .put("exchange.s3.aws-secret-key", MinioStorage.SECRET_KEY)
                .put("exchange.s3.region", "us-east-1")
                // TODO: enable exchange encryption after https is supported for Trino MinIO
                .put("exchange.s3.endpoint", "http://" + minioStorage.getMinio().getMinioApiEndpoint())
                .build();
        return new FileSystemExchangeManagerFactory().create(exchangeManagerProperties);
    }

    @AfterClass(alwaysRun = true)
    public void destroy()
            throws Exception
    {
        if (minioStorage != null) {
            minioStorage.close();
            minioStorage = null;
        }
    }
}
