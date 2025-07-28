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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.block.TestingSession;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.retries.api.RefreshRetryTokenRequest;
import software.amazon.awssdk.retries.api.RefreshRetryTokenResponse;
import software.amazon.awssdk.retries.api.RetryStrategy;
import software.amazon.awssdk.retries.internal.DefaultRetryToken;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestHiveConcurrentModificationGlueMetastore
{
    @Test
    public void testGlueClientShouldRetryConcurrentModificationException()
    {
        GlueHiveMetastoreConfig config = new GlueHiveMetastoreConfig();

        GlueClientFactory glueClientFactory = new GlueClientFactory(
                config,
                Optional.empty(),
                ImmutableSet.of());
        ConnectorSession session = TestingSession.SESSION;
        ConnectorIdentity identity = session.getIdentity();

        try (GlueClient glueClient = glueClientFactory.create(identity)) {
            ClientOverrideConfiguration clientOverrideConfiguration = glueClient.serviceClientConfiguration().overrideConfiguration();
            RetryStrategy retryStrategy = clientOverrideConfiguration.retryStrategy().orElseThrow();

            assertThatThrownBy(() -> retryStrategy.refreshRetryToken(
                    RefreshRetryTokenRequest.builder()
                            .token(DefaultRetryToken.builder().scope("test").build())
                            .failure(new RuntimeException("This is not retryable exception so it should fail"))
                            .build()))
                    .hasMessage("Request attempt 1 encountered non-retryable failure");

            RefreshRetryTokenResponse refreshRetryTokenResponse = retryStrategy.refreshRetryToken(
                    RefreshRetryTokenRequest.builder()
                            .token(DefaultRetryToken.builder().scope("test").build())
                            .failure(
                                    ConcurrentModificationException.builder()
                                            .awsErrorDetails(AwsErrorDetails.builder()
                                                    // taken from software.amazon.awssdk.services.glue.DefaultGlueClient and
                                                    // software.amazon.awssdk.services.glue.DefaultGlueAsyncClient
                                                    .errorCode("ConcurrentModificationException")
                                                    .build())
                                            .message("Test-simulated metastore concurrent modification exception that should be allowed to retry")
                                            .build())
                            .build());
            assertThat(refreshRetryTokenResponse).isNotNull();
        }
    }
}
