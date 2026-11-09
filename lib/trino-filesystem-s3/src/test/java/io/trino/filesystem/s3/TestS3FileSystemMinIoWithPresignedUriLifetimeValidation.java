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

/**
 * Runs the file system tests against MinIO with {@code s3.presigned-uri.lifetime-validation.enabled}, so that
 * presigned URIs are created by {@link S3PresignedUriGenerator} and not by the AWS SDK presigner directly.
 */
public class TestS3FileSystemMinIoWithPresignedUriLifetimeValidation
        extends TestS3FileSystemMinIo
{
    @Override
    protected S3FileSystemConfig createS3FileSystemConfig()
    {
        return super.createS3FileSystemConfig()
                .setPresignedUriLifetimeValidationEnabled(true);
    }
}
