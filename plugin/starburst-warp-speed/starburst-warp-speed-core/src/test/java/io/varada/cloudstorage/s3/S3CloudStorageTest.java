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
package io.varada.cloudstorage.s3;

import io.airlift.configuration.ConfigurationFactory;
import io.airlift.units.DataSize;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.s3.S3FileSystemConfig;
import io.trino.filesystem.s3.S3FileSystemFactory;
import io.trino.spi.connector.ConnectorContext;
import io.varada.annotation.Default;
import io.varada.cloudstorage.CloudStorageAbstractTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;

import java.util.Map;

@Disabled
public class S3CloudStorageTest
        extends CloudStorageAbstractTest
{
//    public static final String ACCESS_KEY = null;
//    public static final String SECRET_KEY = null;
    public static final String IAM_ROLE = "";
    public static final String EXTERNAL_ID = "";
    public static final String REGION = "us-east-1";
    public static final String BUCKET_NAME = "varadaio-us-east1-unit-tests";
    public static final String EMPTY_BUCKET_NAME = "varadaio-us-east1-mount-point-try1";

    @BeforeEach
    void setUp()
    {
        System.setProperty("aws.profile", "varada");

        S3FileSystemConfig config = new S3FileSystemConfig()
//                .setAwsAccessKey(ACCESS_KEY)
//                .setAwsSecretKey(SECRET_KEY)
                .setRegion(REGION)
                .setIamRole(IAM_ROLE)
                .setExternalId(EXTERNAL_ID)
                .setRoleSessionName("S3CloudStorageTest@warp-speed")
                .setStreamingPartSize(DataSize.valueOf("5.5MB"));

        S3FileSystemFactory fileSystemFactory = new S3FileSystemFactory(OpenTelemetry.noop(), config);

        S3CloudStorageModule module = new S3CloudStorageModule(new TestingConnectorContext(), new ConfigurationFactory(Map.of()), Default.class);

        cloudStorage = module.provideS3CloudStorage(fileSystemFactory, config);
    }

    @Override
    protected String getBucket()
    {
        return "s3://" + BUCKET_NAME + "/";
    }

    @Override
    protected String getEmptyBucket()
    {
        return "s3://" + EMPTY_BUCKET_NAME + "/";
    }

    @Override
    protected String getNotExistBucket()
    {
        return "s3://" + BUCKET_NAME + "-not-exist/";
    }

    static class TestingConnectorContext
            implements ConnectorContext
    {
    }
}
