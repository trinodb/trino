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
import io.airlift.configuration.ConfigurationMetadata;
import io.trino.plugin.opa.HttpClientUtils.InstrumentedHttpClient;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;

import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.trino.plugin.opa.TestConstants.BAD_REQUEST_RESPONSE;
import static io.trino.plugin.opa.TestConstants.MALFORMED_RESPONSE;
import static io.trino.plugin.opa.TestConstants.SERVER_ERROR_RESPONSE;
import static io.trino.plugin.opa.TestConstants.SYSTEM_ACCESS_CONTROL_CONTEXT;
import static io.trino.plugin.opa.TestConstants.UNDEFINED_RESPONSE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public final class TestHelpers
{
    private TestHelpers() {}

    public static InstrumentedHttpClient createMockHttpClient(URI expectedUri, Function<JsonNode, MockResponse> handler)
    {
        return new InstrumentedHttpClient(expectedUri, "POST", JSON_UTF_8.toString(), handler);
    }

    public static OpaAccessControl createOpaAuthorizer(OpaConfig config, InstrumentedHttpClient mockHttpClient)
    {
        return (OpaAccessControl) OpaAccessControlFactory.create(opaConfigToDict(config), Optional.of(mockHttpClient), Optional.of(SYSTEM_ACCESS_CONTROL_CONTEXT));
    }

    public static void assertAccessControlMethodThrowsForIllegalResponses(Consumer<OpaAccessControl> methodToTest, OpaConfig opaConfig, URI expectedUri)
    {
        runIllegalResponseTestCases(methodToTest, opaConfig, expectedUri);
    }

    public static void assertAccessControlMethodThrowsForResponse(
            MockResponse response,
            URI expectedUri,
            OpaConfig opaConfig,
            Consumer<OpaAccessControl> methodToTest,
            Class<? extends OpaQueryException> expectedException,
            String expectedErrorMessage)
    {
        InstrumentedHttpClient httpClient = createMockHttpClient(expectedUri, request -> response);
        assertThatThrownBy(() -> methodToTest.accept(createOpaAuthorizer(opaConfig, httpClient)))
                .isInstanceOf(expectedException)
                .hasMessageContaining(expectedErrorMessage);
        assertThat(httpClient.getRequests()).isNotEmpty();
    }

    public static Map<String, String> opaConfigToDict(OpaConfig config)
    {
        ConfigurationMetadata<OpaConfig> metadata = ConfigurationMetadata.getValidConfigurationMetadata(OpaConfig.class);
        ImmutableMap.Builder<String, String> opaConfigBuilder = ImmutableMap.builder();
        try {
            for (ConfigurationMetadata.AttributeMetadata attribute : metadata.getAttributes().values()) {
                convertPropertyToString(attribute.getGetter().invoke(config)).ifPresent(
                        propertyValue -> opaConfigBuilder.put(attribute.getInjectionPoint().getProperty(), propertyValue));
            }
        }
        catch (InvocationTargetException | IllegalAccessException e) {
            throw new AssertionError("Failed to build config map", e);
        }
        return opaConfigBuilder.buildOrThrow();
    }

    private static Optional<String> convertPropertyToString(Object value)
    {
        if (value instanceof Optional<?> optionalValue) {
            return optionalValue.map(Object::toString);
        }
        return Optional.ofNullable(value).map(Object::toString);
    }

    private static void runIllegalResponseTestCases(Consumer<OpaAccessControl> methodToTest, OpaConfig opaConfig, URI expectedUri)
    {
        assertAccessControlMethodThrowsForResponse(UNDEFINED_RESPONSE, expectedUri, opaConfig, methodToTest, OpaQueryException.OpaServerError.PolicyNotFound.class, "did not return a value");
        assertAccessControlMethodThrowsForResponse(BAD_REQUEST_RESPONSE, expectedUri, opaConfig, methodToTest, OpaQueryException.OpaServerError.class, "returned status 400");
        assertAccessControlMethodThrowsForResponse(SERVER_ERROR_RESPONSE, expectedUri, opaConfig, methodToTest, OpaQueryException.OpaServerError.class, "returned status 500");
        assertAccessControlMethodThrowsForResponse(MALFORMED_RESPONSE, expectedUri, opaConfig, methodToTest, OpaQueryException.class, "Failed to deserialize");
    }
}
