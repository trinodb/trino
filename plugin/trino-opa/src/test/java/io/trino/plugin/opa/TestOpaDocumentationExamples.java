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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;

import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
final class TestOpaDocumentationExamples
{
    private static final Path DOCS_DIRECTORY = Path.of("../../docs/src/main/sphinx/security");
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Container
    private static final OpaContainer OPA_CONTAINER = new OpaContainer();

    @Test
    void testBatchColumnMaskingExample()
            throws IOException, InterruptedException
    {
        OPA_CONTAINER.submitPolicy(Files.readString(DOCS_DIRECTORY.resolve("opa-batch-column-masking.rego")));
        JsonNode request = OBJECT_MAPPER.readTree(DOCS_DIRECTORY.resolve("opa-batch-column-masking-request.json").toFile());
        JsonNode expectedResponse = OBJECT_MAPPER.readTree(DOCS_DIRECTORY.resolve("opa-batch-column-masking-response.json").toFile());

        JsonNode actualResponse = queryPolicy("batchColumnMasks", request);

        assertThat(actualResponse.isArray()).isTrue();
        assertEqualsIgnoreOrder(actualResponse, expectedResponse);
    }

    private static JsonNode queryPolicy(String policyName, JsonNode input)
            throws IOException, InterruptedException
    {
        ObjectNode body = OBJECT_MAPPER.createObjectNode();
        body.set("input", input);
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(OPA_CONTAINER.getOpaUriForPolicyPath(policyName))
                        .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(body)))
                        .header("Content-Type", "application/json")
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).withFailMessage("OPA query failed: %s", response.body()).isEqualTo(200);
        return OBJECT_MAPPER.readTree(response.body()).path("result");
    }
}
