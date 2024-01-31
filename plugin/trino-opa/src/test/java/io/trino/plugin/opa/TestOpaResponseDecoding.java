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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.plugin.opa.schema.OpaBatchQueryResult;
import io.trino.plugin.opa.schema.OpaQueryResult;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestOpaResponseDecoding
{
    private final JsonCodec<OpaQueryResult> responseCodec = new JsonCodecFactory().jsonCodec(OpaQueryResult.class);
    private final JsonCodec<OpaBatchQueryResult> batchResponseCodec = new JsonCodecFactory().jsonCodec(OpaBatchQueryResult.class);

    @Test
    public void testCanDeserializeOpaSingleResponse()
    {
        testCanDeserializeOpaSingleResponse(true);
        testCanDeserializeOpaSingleResponse(false);
    }

    private void testCanDeserializeOpaSingleResponse(boolean response)
    {
        OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "decision_id": "foo",
                    "result": %s
                }""".formatted(String.valueOf(response)));
        assertThat(response).isEqualTo(result.result());
        assertThat(result.decisionId()).isEqualTo("foo");
    }

    @Test
    public void testCanDeserializeOpaSingleResponseWithNoDecisionId()
    {
        testCanDeserializeOpaSingleResponseWithNoDecisionId(true);
        testCanDeserializeOpaSingleResponseWithNoDecisionId(false);
    }

    private void testCanDeserializeOpaSingleResponseWithNoDecisionId(boolean response)
    {
        OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "result": %s
                }""".formatted(String.valueOf(response)));
        assertThat(response).isEqualTo(result.result());
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testSingleResponseWithExtraFields()
    {
        OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "result": true,
                    "someExtraInfo": ["foo"]
                }""");
        assertThat(result.result()).isTrue();
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testUndefinedDecisionSingleResponseTreatedAsDeny()
    {
        OpaQueryResult result = this.responseCodec.fromJson("{}");
        assertThat(result.result()).isFalse();
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testEmptyOrUndefinedResponses()
    {
        testEmptyOrUndefinedResponses("{}");
        testEmptyOrUndefinedResponses("{\"result\": []}");
    }

    private void testEmptyOrUndefinedResponses(String response)
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson(response);
        assertThat(result.result()).isEmpty();
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testBatchResponseWithItemsNoDecisionId()
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3]
                }""");
        assertThat(result.result()).containsExactly(1, 2, 3);
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testBatchResponseWithItemsAndDecisionId()
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3],
                    "decision_id": "foobar"
                }""");
        assertThat(result.result()).containsExactly(1, 2, 3);
        assertThat(result.decisionId()).isEqualTo("foobar");
    }

    @Test
    public void testBatchResponseWithExtraFields()
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3],
                    "decision_id": "foobar",
                    "someInfo": "foo",
                    "andAnObject": {}
                }""");
        assertThat(result.result()).containsExactly(1, 2, 3);
        assertThat(result.decisionId()).isEqualTo("foobar");
    }
}
