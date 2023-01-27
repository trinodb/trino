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
package io.trino.plugin.hive.s3;

import org.apache.hadoop.conf.Configuration;
import org.testng.annotations.BeforeClass;

import static io.trino.hadoop.ConfigurationInstantiator.newEmptyConfiguration;
import static java.util.Objects.requireNonNull;

/**
 * Tests file system operations on AWS S3 storage.
 * <p>
 * Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
 * See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
 */
public class TestTrinoS3FileSystemAwsS3
        extends BaseTestTrinoS3FileSystemObjectStorage
{
    private String bucketName;
    private String s3Endpoint;

    @BeforeClass
    public void setup()
    {
        bucketName = requireNonNull(System.getenv("S3_BUCKET"), "Environment S3_BUCKET was not set");
        s3Endpoint = requireNonNull(System.getenv("S3_BUCKET_ENDPOINT"), "Environment S3_BUCKET_ENDPOINT was not set");
    }

    @Override
    protected String getBucketName()
    {
        return bucketName;
    }

    @Override
    protected Configuration s3Configuration()
    {
        Configuration config = newEmptyConfiguration();
        config.set("fs.s3.endpoint", s3Endpoint);
        return config;
    }
}
