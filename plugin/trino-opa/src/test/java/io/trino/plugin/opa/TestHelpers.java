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
package io.trino.plugin.opa;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;

import java.net.URI;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.opa.TestConstants.BAD_REQUEST_RESPONSE;
import static io.trino.plugin.opa.TestConstants.MALFORMED_RESPONSE;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_BATCH_URI;
import static io.trino.plugin.opa.TestConstants.OPA_SERVER_URI;
import static io.trino.plugin.opa.TestConstants.SERVER_ERROR_RESPONSE;
import static io.trino.plugin.opa.TestConstants.SYSTEM_ACCESS_CONTROL_CONTEXT;
import static io.trino.plugin.opa.TestConstants.UNDEFINED_RESPONSE;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TestHelpers
{
    private TestHelpers() {}

    public static InstrumentedHttpClient createMockHttpClient(URI expectedUri, Function<JsonNode, MockResponse> handler)
    {
        return new InstrumentedHttpClient(expectedUri, "POST", JSON_UTF_8.toString(), handler);
    }

    public static OpaAccessControl createOpaAuthorizer(URI opaUri, InstrumentedHttpClient mockHttpClient)
    {
        return (OpaAccessControl) OpaAccessControlFactory.create(ImmutableMap.of("opa.policy.uri", opaUri.toString()), Optional.of(mockHttpClient), Optional.of(SYSTEM_ACCESS_CONTROL_CONTEXT));
    }

    public static OpaAccessControl createOpaAuthorizer(URI opaUri, URI opaBatchUri, InstrumentedHttpClient mockHttpClient)
    {
        return (OpaAccessControl) OpaAccessControlFactory.create(
                ImmutableMap.<String, String>builder()
                        .put("opa.policy.uri", opaUri.toString())
                        .put("opa.policy.batched-uri", opaBatchUri.toString())
                        .buildOrThrow(),
                Optional.of(mockHttpClient),
                Optional.of(SYSTEM_ACCESS_CONTROL_CONTEXT));
    }

    public static void assertAccessControlMethodThrowsForIllegalResponses(Consumer<OpaAccessControl> methodToTest)
    {
        runIllegalResponseTestCases(methodToTest, TestHelpers::buildAuthorizerWithPredefinedResponse);
    }

    public static void assertBatchAccessControlMethodThrowsForIllegalResponses(Consumer<OpaAccessControl> methodToTest)
    {
        runIllegalResponseTestCases(methodToTest, TestHelpers::buildBatchAuthorizerWithPredefinedResponse);
    }

    public static OpaAccessControl buildBatchAuthorizerWithPredefinedResponse(MockResponse response)
    {
        return createOpaAuthorizer(OPA_SERVER_URI, OPA_SERVER_BATCH_URI, createMockHttpClient(OPA_SERVER_BATCH_URI, request -> response));
    }

    public static void assertAccessControlMethodThrows(
            Runnable methodToTest,
            Class<? extends OpaQueryException> expectedException,
            String expectedErrorMessage)
    {
        assertThatThrownBy(methodToTest::run)
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
    }

    private static void runIllegalResponseTestCases(
            Consumer<OpaAccessControl> methodToTest,
            Function<MockResponse, OpaAccessControl> authorizerBuilder)
    {
        assertAccessControlMethodThrows(() -> methodToTest.accept(authorizerBuilder.apply(UNDEFINED_RESPONSE)), OpaQueryException.OpaServerError.PolicyNotFound.class, "did not return a value");
        assertAccessControlMethodThrows(() -> methodToTest.accept(authorizerBuilder.apply(BAD_REQUEST_RESPONSE)), OpaQueryException.OpaServerError.class, "returned status 400");
        assertAccessControlMethodThrows(() -> methodToTest.accept(authorizerBuilder.apply(SERVER_ERROR_RESPONSE)), OpaQueryException.OpaServerError.class, "returned status 500");
        assertAccessControlMethodThrows(() -> methodToTest.accept(authorizerBuilder.apply(MALFORMED_RESPONSE)), OpaQueryException.class, "Failed to deserialize");
    }

    private static OpaAccessControl buildAuthorizerWithPredefinedResponse(MockResponse response)
    {
        return createOpaAuthorizer(OPA_SERVER_URI, createMockHttpClient(OPA_SERVER_URI, request -> response));
    }
}
