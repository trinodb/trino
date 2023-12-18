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
import io.trino.plugin.opa.schema.OpaColumnMaskQueryResult;
import io.trino.plugin.opa.schema.OpaQueryResult;
import io.trino.plugin.opa.schema.OpaRowFiltersQueryResult;
import io.trino.plugin.opa.schema.OpaViewExpression;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestOpaResponseDecoding
{
    private final JsonCodec<OpaQueryResult> responseCodec = new JsonCodecFactory().jsonCodec(OpaQueryResult.class);
    private final JsonCodec<OpaBatchQueryResult> batchResponseCodec = new JsonCodecFactory().jsonCodec(OpaBatchQueryResult.class);
    private final JsonCodec<OpaRowFiltersQueryResult> rowFilteringResponseCodec = new JsonCodecFactory().jsonCodec(OpaRowFiltersQueryResult.class);
    private final JsonCodec<OpaColumnMaskQueryResult> columnMaskingResponseCodec = new JsonCodecFactory().jsonCodec(OpaColumnMaskQueryResult.class);

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
    public void testIllegalResponseThrows()
    {
        testIllegalResponseDecodingThrows("{\"result\": \"foo\"}", responseCodec);
    }

    @Test
    public void testBatchEmptyOrUndefinedResponses()
    {
        testBatchEmptyOrUndefinedResponses("{}");
        testBatchEmptyOrUndefinedResponses("{\"result\": []}");
    }

    private void testBatchEmptyOrUndefinedResponses(String response)
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
    public void testBatchResponseIllegalResponseThrows()
    {
        testIllegalResponseDecodingThrows("""
                {
                    "result": ["foo"],
                    "decision_id": "foobar"
                }""", batchResponseCodec);
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

    @Test
    public void testRowFilteringEmptyOrUndefinedResponses()
    {
        testRowFilteringEmptyOrUndefinedResponses("{}");
        testRowFilteringEmptyOrUndefinedResponses("{\"result\": []}");
    }

    private void testRowFilteringEmptyOrUndefinedResponses(String response)
    {
        OpaRowFiltersQueryResult result = this.rowFilteringResponseCodec.fromJson(response);
        assertThat(result.result()).isEmpty();
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testRowFilteringResponseWithItemsNoDecisionId()
    {
        OpaRowFiltersQueryResult result = this.rowFilteringResponseCodec.fromJson("""
                {
                    "result": [
                        {"expression": "foo"},
                        {"expression": "bar", "identity": "some_identity"}
                    ]
                }""");
        assertThat(result.result()).containsExactlyInAnyOrder(
                new OpaViewExpression("foo", Optional.empty()),
                new OpaViewExpression("bar", Optional.of("some_identity")));
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testRowFilteringResponseWithItemsAndDecisionId()
    {
        OpaRowFiltersQueryResult result = this.rowFilteringResponseCodec.fromJson("""
                {
                    "result": [{"expression": "test_expression"}],
                    "decision_id": "some_id"
                }""");
        assertThat(result.result()).containsExactly(new OpaViewExpression("test_expression", Optional.empty()));
        assertThat(result.decisionId()).isEqualTo("some_id");
    }

    @Test
    public void testRowFilteringResponseWithExtraFields()
    {
        OpaRowFiltersQueryResult result = this.rowFilteringResponseCodec.fromJson("""
                {
                    "result": [{"expression": "test_expression"}],
                    "decision_id": "foobar",
                    "someInfo": "foo",
                    "andAnObject": {}
                }""");
        assertThat(result.result()).containsExactly(new OpaViewExpression("test_expression", Optional.empty()));
        assertThat(result.decisionId()).isEqualTo("foobar");
    }

    @Test
    public void testRowFilteringResponseIllegalResponseThrows()
    {
        testIllegalResponseDecodingThrows("""
               {
                   "result": ["foo"]
               }""", rowFilteringResponseCodec);
    }

    @Test
    public void testColumnMaskingEmptyOrUndefinedResponse()
    {
        OpaColumnMaskQueryResult emptyResult = columnMaskingResponseCodec.fromJson("{}");
        assertThat(emptyResult.result()).isEmpty();
        assertThat(emptyResult.decisionId()).isNull();
        OpaColumnMaskQueryResult undefinedResult = columnMaskingResponseCodec.fromJson("{\"result\": null}");
        assertThat(undefinedResult.result()).isEmpty();
        assertThat(undefinedResult.decisionId()).isNull();
    }

    @Test
    public void testColumnMaskingResponsesWithNoDecisionId()
    {
        OpaColumnMaskQueryResult result = this.columnMaskingResponseCodec.fromJson("""
                {
                    "result": {"expression": "test_expression"}
                }""");
        assertThat(result.result()).contains(new OpaViewExpression("test_expression", Optional.empty()));
        assertThat(result.decisionId()).isNull();
    }

    @Test
    public void testColumnMaskingResponsesWithDecisionId()
    {
        OpaColumnMaskQueryResult resultWithExpression = this.columnMaskingResponseCodec.fromJson("""
                {
                    "result": {"expression": "test_expression"},
                    "decision_id": "foobar"
                }""");
        OpaColumnMaskQueryResult resultWithExpressionAndIdentity = this.columnMaskingResponseCodec.fromJson("""
                {
                    "result": {"expression": "test_expression", "identity": "some_identity"},
                    "decision_id": "foobar"
                }""");
        assertThat(resultWithExpression.result()).contains(new OpaViewExpression("test_expression", Optional.empty()));
        assertThat(resultWithExpressionAndIdentity.result()).contains(new OpaViewExpression("test_expression", Optional.of("some_identity")));
        assertThat(resultWithExpression.decisionId()).isEqualTo("foobar");
        assertThat(resultWithExpressionAndIdentity.decisionId()).isEqualTo("foobar");
    }

    @Test
    public void testColumnMaskingResponseWithExtraFields()
    {
        OpaColumnMaskQueryResult result = this.columnMaskingResponseCodec.fromJson("""
                {
                    "result": {"expression": "test_expression"},
                    "decision_id": "foobar",
                    "someInfo": "foo",
                    "andAnObject": {}
                }""");
        assertThat(result.result()).contains(new OpaViewExpression("test_expression", Optional.empty()));
        assertThat(result.decisionId()).isEqualTo("foobar");
    }

    @Test
    public void testColumnMaskingResponseIllegalResponseThrows()
    {
        testIllegalResponseDecodingThrows("""
               {
                   "result": {"foo": "bar"}
               }""", columnMaskingResponseCodec);
    }

    private <T> void testIllegalResponseDecodingThrows(String rawResponse, JsonCodec<T> codec)
    {
        assertThatThrownBy(() -> codec.fromJson(rawResponse))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Invalid JSON");
    }
}
