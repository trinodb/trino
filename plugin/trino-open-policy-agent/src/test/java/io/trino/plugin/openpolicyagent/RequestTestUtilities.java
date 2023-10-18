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
package io.trino.plugin.openpolicyagent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class RequestTestUtilities
{
    private RequestTestUtilities() {}

    private static final JsonMapper jsonMapper = new JsonMapper();

    public static void assertStringRequestsEqual(
            Collection<String> expectedRequests, Collection<String> actualRequests, String extractPath)
    {
        Set<JsonNode> parsedExpectedRequests = expectedRequests.stream()
                .map(expectedRequest -> {
                    try {
                        return jsonMapper.readTree(expectedRequest);
                    }
                    catch (IOException e) {
                        fail("Could not parse request", e);
                        return null;
                    }
                })
                .collect(toImmutableSet());
        assertJsonRequestsEqual(parsedExpectedRequests, actualRequests, extractPath);
    }

    public static void assertJsonRequestsEqual(
            Collection<JsonNode> expectedRequests, Collection<String> actualRequests, String extractPath)
    {
        assertEquals(
                expectedRequests.size(),
                actualRequests.size(),
                "Mismatch in expected vs. actual request count");

        Set<JsonNode> parsedActualRequests = actualRequests.stream()
                .map(actualRequest -> {
                    try {
                        JsonNode parsed = jsonMapper.readTree(actualRequest);
                        if (extractPath != null) {
                            return parsed.at(extractPath);
                        }
                        return parsed;
                    }
                    catch (IOException e) {
                        fail("Could not parse request", e);
                        return null;
                    }
                })
                .collect(toImmutableSet());
        assertEquals(ImmutableSet.copyOf(expectedRequests), parsedActualRequests, "Requests do not match");
    }
}
