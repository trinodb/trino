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
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;

import java.util.Map;
import java.util.function.Supplier;

import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY;
import static io.trino.filesystem.s3.S3FileSystemConstants.EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY;
import static java.util.Objects.requireNonNull;

final class SupplierAwsCredentialsProvider
        implements AwsCredentialsProvider
{
    private final Supplier<Map<String, String>> credentialsRefresher;

    SupplierAwsCredentialsProvider(Supplier<Map<String, String>> credentialsRefresher)
    {
        this.credentialsRefresher = requireNonNull(credentialsRefresher, "credentialsRefresher is null");
    }

    @Override
    public AwsCredentials resolveCredentials()
    {
        Map<String, String> extraCredentials = credentialsRefresher.get();
        return AwsSessionCredentials.create(
                extraCredentials.get(EXTRA_CREDENTIALS_ACCESS_KEY_PROPERTY),
                extraCredentials.get(EXTRA_CREDENTIALS_SECRET_KEY_PROPERTY),
                extraCredentials.get(EXTRA_CREDENTIALS_SESSION_TOKEN_PROPERTY));
    }
}
