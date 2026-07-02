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
package io.trino.plugin.doris;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestDorisQueryPlanResponse
{
    private static final JsonCodec<DorisQueryPlanResponse> QUERY_PLAN_RESPONSE_CODEC = new JsonCodecFactory().jsonCodec(DorisQueryPlanResponse.class);

    @Test
    void testIgnoresUnknownTabletFields()
    {
        String json =
                """
                {
                  "status": 200,
                  "opaqued_query_plan": "opaque-plan",
                  "partitions": {
                    "13989": {
                      "routings": ["10.0.0.1:9060"],
                      "version": 12
                    }
                  },
                  "meta": {
                    "request_id": "abc"
                  }
                }
                """;

        DorisQueryPlanResponse response = QUERY_PLAN_RESPONSE_CODEC.fromJson(json);

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.opaquedQueryPlan()).isEqualTo("opaque-plan");
        assertThat(response.partitions().get("13989"))
                .isEqualTo(new DorisQueryPlanTablet(List.of("10.0.0.1:9060")));
    }

    @Test
    void testTabletRoutingsAreDefensivelyCopied()
    {
        List<String> routings = new ArrayList<>(List.of("10.0.0.1:9060"));

        DorisQueryPlanTablet tablet = new DorisQueryPlanTablet(routings);
        routings.add("10.0.0.2:9060");

        assertThat(tablet.routings()).containsExactly("10.0.0.1:9060");
        assertThatThrownBy(() -> tablet.routings().add("10.0.0.3:9060"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
