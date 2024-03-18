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
package io.varada.cloudstorage.gcs;

import io.airlift.configuration.ConfigurationFactory;
import io.trino.filesystem.gcs.GcsFileSystemConfig;
import io.trino.filesystem.gcs.GcsFileSystemFactory;
import io.trino.filesystem.gcs.GcsStorageFactory;
import io.trino.spi.connector.ConnectorContext;
import io.varada.annotation.Default;
import io.varada.cloudstorage.CloudStorageAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import java.io.IOException;
import java.util.Map;

@Disabled
public class GcsCloudStorageTest
        extends CloudStorageAbstractTest
{
    public static final String GCP_CREDENTIAL_KEY = "";
    public static final String BUCKET_NAME = "siactestdata";
    public static final String EMPTY_BUCKET_NAME = "yaffi-test";

    @BeforeEach
    void setUp()
            throws IOException
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setJsonKey(GCP_CREDENTIAL_KEY);

        GcsStorageFactory storageFactory = new GcsStorageFactory(config);

        GcsFileSystemFactory fileSystemFactory = new GcsFileSystemFactory(config, storageFactory);

        GcsCloudStorageModule module = new GcsCloudStorageModule(new TestingConnectorContext(), new ConfigurationFactory(Map.of()), Default.class);

        cloudStorage = module.provideGcsCloudStorage(fileSystemFactory, storageFactory);
    }

    @Override
    protected String getBucket()
    {
        return "gs://" + BUCKET_NAME + "/";
    }

    @Override
    protected String getEmptyBucket()
    {
        return "gs://" + EMPTY_BUCKET_NAME + "/";
    }

    @Override
    protected String getNotExistBucket()
    {
        return "gs://" + BUCKET_NAME + "-not-exist/";
    }

    static class TestingConnectorContext
            implements ConnectorContext
    {
    }
}
