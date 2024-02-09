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
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.opa.HttpClientUtils.MockResponse;
import io.trino.spi.security.Identity;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.opa.TestConstants.SYSTEM_ACCESS_CONTROL_CONTEXT;
import static org.assertj.core.api.Assertions.assertThat;

public final class RequestTestUtilities
{
    private RequestTestUtilities() {}

    private static final JsonMapper jsonMapper = new JsonMapper();

    public static void assertStringRequestsEqual(Set<String> expectedRequests, Collection<JsonNode> actualRequests, String extractPath)
    {
        Set<JsonNode> parsedExpectedRequests = expectedRequests.stream()
                .map(expectedRequest -> {
                    try {
                        return jsonMapper.readTree(expectedRequest);
                    }
                    catch (IOException e) {
                        throw new AssertionError("Cannot parse expected request", e);
                    }
                })
                .collect(toImmutableSet());
        Set<JsonNode> extractedActualRequests = actualRequests.stream().map(node -> node.at(extractPath)).collect(toImmutableSet());
        assertThat(extractedActualRequests).containsExactlyInAnyOrderElementsOf(parsedExpectedRequests);
    }

    public static Function<JsonNode, MockResponse> buildValidatingRequestHandler(Identity expectedUser, int statusCode, String responseContents)
    {
        return buildValidatingRequestHandler(expectedUser, new MockResponse(responseContents, statusCode));
    }

    public static Function<JsonNode, MockResponse> buildValidatingRequestHandler(Identity expectedUser, MockResponse response)
    {
        return buildValidatingRequestHandler(expectedUser, jsonNode -> response);
    }

    public static Function<JsonNode, MockResponse> buildValidatingRequestHandler(Identity expectedUser, Function<JsonNode, MockResponse> customHandler)
    {
        return parsedRequest -> {
            if (!parsedRequest.at("/input/context/identity/user").asText().equals(expectedUser.getUser())) {
                throw new AssertionError("Request had invalid user in the identity block");
            }
            ImmutableSet.Builder<String> groupsInRequestBuilder = ImmutableSet.builder();
            parsedRequest.at("/input/context/identity/groups").iterator().forEachRemaining(node -> groupsInRequestBuilder.add(node.asText()));
            if (!groupsInRequestBuilder.build().equals(expectedUser.getGroups())) {
                throw new AssertionError("Request had invalid set of groups in the identity block");
            }
            if (!parsedRequest.at("/input/context/softwareStack/trinoVersion").asText().equals(SYSTEM_ACCESS_CONTROL_CONTEXT.getVersion())) {
                throw new AssertionError("Request had invalid trinoVersion");
            }
            return customHandler.apply(parsedRequest);
        };
    }
}
