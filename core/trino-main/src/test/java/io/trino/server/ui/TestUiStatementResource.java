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
package io.trino.server.ui;

import io.trino.client.QueryResults;
import io.trino.client.StatementStats;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;

import static io.trino.server.ui.UiStatementResource.withUiNextUri;
import static org.assertj.core.api.Assertions.assertThat;

class TestUiStatementResource
{
    @Test
    void testContinuationUris()
    {
        for (String prefix : List.of("", "/proxy", "/proxy%20prefix")) {
            for (String state : List.of("queued", "executing")) {
                URI nextUri = URI.create("https://coordinator:8443" + prefix + "/v1/statement/" + state + "/query/slug/1?key=a%2Fb#fragment");
                QueryResults original = results(nextUri);
                QueryResults adapted = withUiNextUri(original, prefix + "/v1/statement");
                assertThat(adapted.getNextUri()).isEqualTo(URI.create(nextUri.toString().replace("/v1/statement/", "/ui/api/statement/")));
                assertThat(original.getNextUri()).isEqualTo(nextUri);
                assertThat(adapted.getInfoUri()).isEqualTo(original.getInfoUri());
                assertThat(adapted.getPartialCancelUri()).isEqualTo(original.getPartialCancelUri());
                assertThat(adapted.getStats()).isSameAs(original.getStats());
            }
        }
    }

    @Test
    void testTerminalResults()
    {
        QueryResults results = results(null);
        assertThat(withUiNextUri(results, "/v1/statement")).isSameAs(results);
    }

    private static QueryResults results(URI nextUri)
    {
        return new QueryResults(
                "query",
                URI.create("https://coordinator/ui/query.html?query"),
                URI.create("https://coordinator/v1/statement/executing/partialCancel/query/1/slug/1"),
                nextUri,
                null,
                null,
                StatementStats.builder()
                        .setState("RUNNING")
                        .setProgressPercentage(OptionalDouble.empty())
                        .setRunningPercentage(OptionalDouble.empty())
                        .build(),
                null,
                List.of(),
                null,
                OptionalLong.empty());
    }
}
