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
package io.trino.plugin.iceberg.catalog.hms;

import io.trino.plugin.base.util.AutoCloseableCloser;
import io.trino.plugin.hive.containers.Hive4MinioDataLake;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.net.URI;

public class TestTrinoHive4CatalogWithHiveMetastore
        extends TestTrinoHiveCatalogWithHiveMetastore
{
    private final AutoCloseableCloser closer = AutoCloseableCloser.create();
    private Hive4MinioDataLake dataLake;

    @BeforeAll
    public void setUp()
    {
        dataLake = closer.register(new Hive4MinioDataLake(bucketName));
        dataLake.start();
    }

    @AfterAll
    public void tearDown()
            throws Exception
    {
        dataLake = null;
        closer.close();
    }

    @Override
    protected URI hiveMetastoreEndpoint()
    {
        return dataLake.getHiveMetastore().getHiveMetastoreEndpoint();
    }

    @Override
    protected String minioAddress()
    {
        return dataLake.getMinio().getMinioAddress();
    }
}
