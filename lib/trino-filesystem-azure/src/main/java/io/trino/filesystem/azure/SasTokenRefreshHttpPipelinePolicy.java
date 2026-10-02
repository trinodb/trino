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
package io.trino.filesystem.azure;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.http.HttpPipelineCallContext;
import com.azure.core.http.HttpPipelineNextPolicy;
import com.azure.core.http.HttpPipelinePosition;
import com.azure.core.http.HttpResponse;
import com.azure.core.http.policy.HttpPipelinePolicy;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.function.Supplier;

import static com.azure.core.http.HttpPipelinePosition.PER_CALL;
import static io.trino.filesystem.azure.AzureFileSystemConstants.EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX;
import static java.util.Objects.requireNonNull;

public final class SasTokenRefreshHttpPipelinePolicy
        implements HttpPipelinePolicy
{
    private final String storageAccount;
    private final AzureSasCredential credential;
    private final Supplier<Map<String, String>> credentialsRefresher;

    public SasTokenRefreshHttpPipelinePolicy(String storageAccount, AzureSasCredential credential, Supplier<Map<String, String>> credentialsRefresher)
    {
        this.storageAccount = requireNonNull(storageAccount, "storageAccount is null");
        this.credential = requireNonNull(credential, "credential is null");
        this.credentialsRefresher = requireNonNull(credentialsRefresher, "credentialsRefresher is null");
    }

    @Override
    public HttpPipelinePosition getPipelinePosition()
    {
        // it needs to be placed before AzureSasCredentialPolicy in pipeline
        return PER_CALL;
    }

    @Override
    public Mono<HttpResponse> process(HttpPipelineCallContext context, HttpPipelineNextPolicy next)
    {
        String sasToken = credentialsRefresher.get().get(EXTRA_CREDENTIALS_AZURE_SAS_TOKEN_PREFIX + storageAccount);
        if (sasToken != null && !sasToken.equals(credential.getSignature())) {
            credential.update(sasToken);
        }
        return next.process();
    }
}
