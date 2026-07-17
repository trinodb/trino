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
package io.trino.filesystem.s3;

import io.opentelemetry.api.OpenTelemetry;
import software.amazon.awssdk.services.s3.DelegatingS3Client;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.S3Request;

import java.util.function.Function;

import static io.trino.filesystem.s3.S3FileSystemConfig.S3SseType.CUSTOMER;
import static io.trino.filesystem.s3.SseCustomerKeyUtil.generateCustomerKey;
import static io.trino.filesystem.s3.SseCustomerKeyUtil.invokeOperationWithCustomerKey;
import static io.trino.testing.containers.Floci.FLOCI_ACCESS_KEY;
import static io.trino.testing.containers.Floci.FLOCI_REGION;
import static io.trino.testing.containers.Floci.FLOCI_SECRET_KEY;

public class TestS3FileSystemFlociWithSseCustomerKey
        extends TestS3FileSystemFloci
{
    private S3SseCustomerKey s3SseCustomerKey;

    @Override
    protected void initEnvironment()
    {
        s3SseCustomerKey = generateCustomerKey();
        super.initEnvironment();
    }

    @Override
    protected S3Client createS3Client()
    {
        S3Client s3Client = super.createS3Client();

        return new DelegatingS3Client(s3Client)
        {
            @Override
            protected <T extends S3Request, V> V invokeOperation(T request, Function<T, V> operation)
            {
                return invokeOperationWithCustomerKey(request, operation, s3SseCustomerKey);
            }
        };
    }

    @Override
    protected S3FileSystemFactory createS3FileSystemFactory()
    {
        return new S3FileSystemFactory(
                OpenTelemetry.noop(),
                new S3FileSystemConfig()
                        .setAwsAccessKey(FLOCI_ACCESS_KEY)
                        .setAwsSecretKey(FLOCI_SECRET_KEY)
                        .setEndpoint(endpoint().toString())
                        .setRegion(FLOCI_REGION)
                        .setPathStyleAccess(true)
                        .setSseType(CUSTOMER)
                        .setSseCustomerKey(s3SseCustomerKey.key())
                        .setStreamingPartSize(STREAMING_PART_SIZE),
                new S3FileSystemStats());
    }
}
