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
package io.trino.client;

import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.util.HashMap;
import java.util.Map;

import static software.amazon.awssdk.profiles.ProfileProperty.AWS_ACCESS_KEY_ID;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_SECRET_ACCESS_KEY;
import static software.amazon.awssdk.profiles.ProfileProperty.AWS_SESSION_TOKEN;

public class DefaultAwsChainClientExternalCredentialProvider
        implements ClientExternalCredentialProvider
{
    private static final DefaultCredentialsProvider defaultAWSCredentialsProvider = DefaultCredentialsProvider.create();

    @Override
    public Map<String, String> getExternalCredentials()
    {
        Map<String, String> externalAwsCredentials = new HashMap<>();
        try {
            AwsSessionCredentials credentials = (AwsSessionCredentials) defaultAWSCredentialsProvider.resolveCredentials();
            externalAwsCredentials.put(AWS_ACCESS_KEY_ID, credentials.accessKeyId());
            externalAwsCredentials.put(AWS_SECRET_ACCESS_KEY, credentials.secretAccessKey());
            externalAwsCredentials.put(AWS_SESSION_TOKEN, credentials.sessionToken());
        }
        catch (Throwable e) {
            throw new RuntimeException("Unable to fetch aws external credentials", e);
        }
        return externalAwsCredentials;
    }
}
