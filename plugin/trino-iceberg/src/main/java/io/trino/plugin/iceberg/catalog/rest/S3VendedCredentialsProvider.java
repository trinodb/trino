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
package io.trino.plugin.iceberg.catalog.rest;

import org.apache.iceberg.rest.credentials.Credential;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.iceberg.aws.AwsClientProperties.REFRESH_CREDENTIALS_ENABLED;
import static org.apache.iceberg.aws.AwsClientProperties.REFRESH_CREDENTIALS_ENDPOINT;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.ACCESS_KEY_ID;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SECRET_ACCESS_KEY;
import static org.apache.iceberg.aws.s3.S3FileIOProperties.SESSION_TOKEN;

final class S3VendedCredentialsProvider
        extends AbstractIcebergRestVendedCredentialsProvider<S3VendedCredentials>
{
    // Copy org.apache.iceberg.aws.s3.S3FileIOProperties.SESSION_TOKEN_EXPIRES_AT_MS because the apache/iceberg constant is package-private
    static final String SESSION_TOKEN_EXPIRES_AT_MS = "s3.session-token-expires-at-ms";

    S3VendedCredentialsProvider(
            Map<String, String> catalogProperties,
            Map<String, String> fileIoProperties)
    {
        super(
                catalogProperties,
                fileIoProperties,
                parseBoolean(fileIoProperties, REFRESH_CREDENTIALS_ENABLED, true),
                Optional.ofNullable(fileIoProperties.get(REFRESH_CREDENTIALS_ENDPOINT)),
                createVendedCredentials(fileIoProperties));
    }

    @Override
    protected S3VendedCredentials applyRefreshedCredentials(List<Credential> credentials)
    {
        Credential s3Credential = credentials.stream()
                .filter(c -> c.prefix().startsWith("s3"))
                .reduce((a, b) -> {
                    throw new IllegalStateException("Multiple S3 credentials returned");
                })
                .orElseThrow(() -> new IllegalStateException("No S3 credentials in refresh response"));

        Map<String, String> config = s3Credential.config();
        return createVendedCredentials(config);
    }

    public static S3VendedCredentials createVendedCredentials(Map<String, String> fileIoProperties)
    {
        return new S3VendedCredentials(
                fileIoProperties.get(ACCESS_KEY_ID),
                fileIoProperties.get(SECRET_ACCESS_KEY),
                fileIoProperties.get(SESSION_TOKEN),
                parseInstantEpochMillis(fileIoProperties, SESSION_TOKEN_EXPIRES_AT_MS));
    }
}
