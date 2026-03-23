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

import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.rest.RESTClient;
import org.apache.iceberg.rest.RESTRequest;
import org.apache.iceberg.rest.RESTResponse;
import org.apache.iceberg.rest.credentials.ImmutableCredential;
import org.apache.iceberg.rest.responses.ErrorResponse;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.apache.iceberg.PartitionSpec.unpartitioned;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link StorageCredentialsMergingRestClient}, which merges the {@code storage-credentials}
 * field (Iceberg REST spec >= 1.7.0) into the config map so Trino's ioBuilder can access vended credentials.
 */
class TestStorageCredentialsMergingRestClient
{
    private static final Schema TEST_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()));

    private static Stream<Arguments> credentialMergingScenarios()
    {
        return Stream.of(
                // Storage credentials only: credentials are merged into config
                Arguments.of(
                        "storageCredentialsOnly",
                        LoadTableResponse.builder()
                                .withTableMetadata(tableMetadata("s3://bucket/warehouse/table1"))
                                .addCredential(ImmutableCredential.builder()
                                        .prefix("s3://bucket/warehouse/")
                                        .config(ImmutableMap.of("s3.access-key-id", "credential-key"))
                                        .build())
                                .build(),
                        ImmutableMap.of("s3.access-key-id", "credential-key")),
                // Config only: config is unchanged
                Arguments.of(
                        "configOnly",
                        LoadTableResponse.builder()
                                .withTableMetadata(tableMetadata("s3://bucket/warehouse/table1"))
                                .addAllConfig(ImmutableMap.of("existing-key", "existing-value"))
                                .build(),
                        ImmutableMap.of("existing-key", "existing-value")),
                // Both: storage credentials override config
                Arguments.of(
                        "storageCredentialsOverrideConfig",
                        LoadTableResponse.builder()
                                .withTableMetadata(tableMetadata("s3://bucket/warehouse/table1"))
                                .addAllConfig(ImmutableMap.of("s3.access-key-id", "config-key"))
                                .addCredential(ImmutableCredential.builder()
                                        .prefix("s3://bucket/")
                                        .config(ImmutableMap.of("s3.access-key-id", "storage-credential-key"))
                                        .build())
                                .build(),
                        ImmutableMap.of("s3.access-key-id", "storage-credential-key")),
                // Neither: config is empty
                Arguments.of(
                        "noCredentialsNoConfig",
                        LoadTableResponse.builder()
                                .withTableMetadata(tableMetadata("s3://bucket/warehouse/table1"))
                                .build(),
                        ImmutableMap.of()));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("credentialMergingScenarios")
    void testCredentialMerging(String scenario, LoadTableResponse response, Map<String, String> expectedConfig)
    {
        StorageCredentialsMergingRestClient client = new StorageCredentialsMergingRestClient(
                new FakeRestClient(response));

        LoadTableResponse result = client.get(
                "v1/namespaces/ns/tables/table1",
                ImmutableMap.of(),
                LoadTableResponse.class,
                ImmutableMap.of(),
                error -> {});

        assertThat(result.config())
                .containsExactlyInAnyOrderEntriesOf(expectedConfig);
    }

    @Test
    void testLongestPrefixMatchWins()
    {
        LoadTableResponse response = LoadTableResponse.builder()
                .withTableMetadata(tableMetadata("s3://bucket/warehouse/db/table1"))
                .addCredential(ImmutableCredential.builder()
                        .prefix("s3://bucket/")
                        .config(ImmutableMap.of("s3.access-key-id", "short-prefix-key"))
                        .build())
                .addCredential(ImmutableCredential.builder()
                        .prefix("s3://bucket/warehouse/db/")
                        .config(ImmutableMap.of("s3.access-key-id", "long-prefix-key"))
                        .build())
                .build();

        StorageCredentialsMergingRestClient client = new StorageCredentialsMergingRestClient(
                new FakeRestClient(response));

        LoadTableResponse result = client.get(
                "v1/namespaces/ns/tables/table1",
                ImmutableMap.of(),
                LoadTableResponse.class,
                ImmutableMap.of(),
                error -> {});

        assertThat(result.config())
                .containsEntry("s3.access-key-id", "long-prefix-key");
    }

    @Test
    void testNoMatchingPrefixLeavesConfigUnchanged()
    {
        LoadTableResponse response = LoadTableResponse.builder()
                .withTableMetadata(tableMetadata("s3://bucket/warehouse/table1"))
                .addAllConfig(ImmutableMap.of("existing-key", "existing-value"))
                .addCredential(ImmutableCredential.builder()
                        .prefix("s3://other-bucket/")
                        .config(ImmutableMap.of("s3.access-key-id", "wrong-key"))
                        .build())
                .build();

        StorageCredentialsMergingRestClient client = new StorageCredentialsMergingRestClient(
                new FakeRestClient(response));

        LoadTableResponse result = client.get(
                "v1/namespaces/ns/tables/table1",
                ImmutableMap.of(),
                LoadTableResponse.class,
                ImmutableMap.of(),
                error -> {});

        assertThat(result.config())
                .containsExactlyEntriesOf(ImmutableMap.of("existing-key", "existing-value"));
    }

    private static TableMetadata tableMetadata(String location)
    {
        return TableMetadata.newTableMetadata(TEST_SCHEMA, unpartitioned(), location, ImmutableMap.of());
    }

    private static class FakeRestClient
            implements RESTClient
    {
        private final RESTResponse response;

        FakeRestClient(RESTResponse response)
        {
            this.response = response;
        }

        @Override
        public void head(String path, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T extends RESTResponse> T delete(String path, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            throw new UnsupportedOperationException();
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends RESTResponse> T get(String path, Map<String, String> queryParams, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            return (T) response;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends RESTResponse> T post(String path, RESTRequest body, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            return (T) response;
        }

        @Override
        public <T extends RESTResponse> T postForm(String path, Map<String, String> formData, Class<T> responseType, Map<String, String> headers, Consumer<ErrorResponse> errorHandler)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }
    }
}
