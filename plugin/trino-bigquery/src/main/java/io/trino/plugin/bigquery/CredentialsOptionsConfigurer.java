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
package io.trino.plugin.bigquery;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.storage.v1.BigQueryReadSettings;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class CredentialsOptionsConfigurer
        implements BigQueryOptionsConfigurer
{
    private final BigQueryCredentialsSupplier credentialsSupplier;
    private final Optional<String> parentProjectId;

    @Inject
    public CredentialsOptionsConfigurer(BigQueryConfig bigQueryConfig, BigQueryCredentialsSupplier credentialsSupplier)
    {
        this.parentProjectId = requireNonNull(bigQueryConfig, "bigQueryConfig is null").getParentProjectId();
        this.credentialsSupplier = requireNonNull(credentialsSupplier, "credentialsSupplier is null");
    }

    @Override
    public BigQueryOptions.Builder configure(BigQueryOptions.Builder builder, ConnectorSession session)
    {
        Optional<Credentials> credentials = credentialsSupplier.getCredentials(session);
        String billingProjectId = calculateBillingProjectId(parentProjectId, credentials);
        credentials.ifPresent(builder::setCredentials);
        builder.setProjectId(billingProjectId);
        return builder;
    }

    @Override
    public BigQueryReadSettings.Builder configure(BigQueryReadSettings.Builder builder, ConnectorSession session)
    {
        Optional<Credentials> credentials = credentialsSupplier.getCredentials(session);
        credentials.ifPresent(value ->
                builder.setCredentialsProvider(FixedCredentialsProvider.create(value)));
        return builder;
    }

    // Note that at this point the config has been validated, which means that option 2 or option 3 will always be valid
    @VisibleForTesting
    static String calculateBillingProjectId(Optional<String> configParentProjectId, Optional<Credentials> credentials)
    {
        // 1. Get from configuration
        return configParentProjectId
                // 2. Get from the provided credentials, but only ServiceAccountCredentials contains the project id.
                // All other credentials types (User, AppEngine, GCE, CloudShell, etc.) take it from the environment
                .orElseGet(() -> credentials
                        .filter(ServiceAccountCredentials.class::isInstance)
                        .map(ServiceAccountCredentials.class::cast)
                        .map(ServiceAccountCredentials::getProjectId)
                        // 3. No configuration was provided, so get the default from the environment
                        .orElseGet(BigQueryOptions::getDefaultProjectId));
    }
}
