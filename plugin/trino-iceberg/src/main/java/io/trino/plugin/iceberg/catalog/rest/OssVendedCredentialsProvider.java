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

import static org.apache.iceberg.aliyun.AliyunProperties.CLIENT_ACCESS_KEY_ID;
import static org.apache.iceberg.aliyun.AliyunProperties.CLIENT_ACCESS_KEY_SECRET;
import static org.apache.iceberg.aliyun.AliyunProperties.CLIENT_SECURITY_TOKEN;

final class OssVendedCredentialsProvider
        extends AbstractIcebergRestVendedCredentialsProvider<OssVendedCredentials>
{
    OssVendedCredentialsProvider(
            Map<String, String> catalogProperties,
            Map<String, String> fileIoProperties)
    {
        super(catalogProperties,
                fileIoProperties,
                // Iceberg AliyunProperties does not define a standard refresh endpoint.
                false,
                Optional.empty(),
                createVendedCredentials(fileIoProperties));
    }

    @Override
    protected OssVendedCredentials applyRefreshedCredentials(List<Credential> credentials)
    {
        Credential ossCredential = credentials.stream()
                .filter(credential -> credential.prefix().equals("oss") || credential.prefix().startsWith("oss://"))
                .reduce((_, _) -> {
                    throw new IllegalStateException("Multiple OSS credentials returned");
                })
                .orElseThrow(() -> new IllegalStateException("No OSS credentials in refresh response"));

        return createVendedCredentials(ossCredential.config());
    }

    public static OssVendedCredentials createVendedCredentials(Map<String, String> fileIoProperties)
    {
        return new OssVendedCredentials(
                fileIoProperties.get(CLIENT_ACCESS_KEY_ID),
                fileIoProperties.get(CLIENT_ACCESS_KEY_SECRET),
                Optional.ofNullable(fileIoProperties.get(CLIENT_SECURITY_TOKEN)),
                Optional.empty());
    }
}
