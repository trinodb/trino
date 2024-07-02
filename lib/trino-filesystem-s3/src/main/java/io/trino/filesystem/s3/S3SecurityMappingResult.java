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

import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

record S3SecurityMappingResult(
        Optional<AwsCredentials> credentials,
        Optional<String> iamRole,
        Optional<String> roleSessionName,
        Optional<String> kmsKeyId,
        Optional<String> endpoint,
        Optional<String> region)
{
    public S3SecurityMappingResult
    {
        requireNonNull(credentials, "credentials is null");
        requireNonNull(iamRole, "iamRole is null");
        requireNonNull(roleSessionName, "roleSessionName is null");
        requireNonNull(kmsKeyId, "kmsKeyId is null");
        requireNonNull(endpoint, "endpoint is null");
        requireNonNull(region, "region is null");
    }

    public Optional<AwsCredentialsProvider> credentialsProvider()
    {
        return credentials.map(StaticCredentialsProvider::create);
    }
}
