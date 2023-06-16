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

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.plugin.openpolicyagent.schema.OpaBatchQueryResult;
import io.trino.plugin.openpolicyagent.schema.OpaQueryResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ResponseTest
{
    private JsonCodec<OpaQueryResult> responseCodec;
    private JsonCodec<OpaBatchQueryResult> batchResponseCodec;

    @BeforeEach
    public void setupParser()
    {
        this.responseCodec = new JsonCodecFactory().jsonCodec(OpaQueryResult.class);
        this.batchResponseCodec = new JsonCodecFactory().jsonCodec(OpaBatchQueryResult.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCanDeserializeOpaSingleResponse(boolean response)
    {
        OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "decision_id": "foo",
                    "result": %s
                }""".formatted(String.valueOf(response)));
        assertEquals(response, result.result());
        assertEquals("foo", result.decisionId());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCanDeserializeOpaSingleResponseWithNoDecisionId(boolean response)
    {
        OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "result": %s
                }""".formatted(String.valueOf(response)));
        assertEquals(response, result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testSingleResponseWithExtraFields()
    {
        OpaQueryResult result = this.responseCodec.fromJson("""
                {
                    "result": true,
                    "someExtraInfo": ["foo"]
                }""");
        assertTrue(result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testUndefinedDecisionSingleResponseTreatedAsDeny()
    {
        OpaQueryResult result = this.responseCodec.fromJson("{}");
        assertFalse(result.result());
        assertNull(result.decisionId());
    }

    @ParameterizedTest
    @ValueSource(strings = {"{}", "{\"result\": []}"})
    public void testEmptyOrUndefinedResponses(String response)
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson(response);
        assertEquals(List.of(), result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseWithItemsNoDecisionId()
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3]
                }""");
        assertEquals(List.of(1, 2, 3), result.result());
        assertNull(result.decisionId());
    }

    @Test
    public void testBatchResponseWithItemsAndDecisionId()
    {
        OpaBatchQueryResult result = this.batchResponseCodec.fromJson("""
                {
                    "result": [1, 2, 3],
                    "decision_id": "foobar"
                }""");
        assertEquals(List.of(1, 2, 3), result.result());
        assertEquals("foobar", result.decisionId());
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
        assertEquals(List.of(1, 2, 3), result.result());
        assertEquals("foobar", result.decisionId());
    }
}
