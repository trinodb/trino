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
package io.trino.plugin.prometheus;

import com.google.common.io.Resources;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import static java.time.Instant.ofEpochMilli;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrometheusQueryVectorResponseParse
{
    @Test
    public void trueStatusOnSuccessResponse()
            throws IOException
    {
        try (InputStream promVectorResponse = openStream()) {
            assertThat(new PrometheusQueryResponseParse(promVectorResponse).getStatus()).isTrue();
        }
    }

    @Test
    public void verifyMetricPropertiesResponse()
            throws IOException
    {
        try (InputStream promVectorResponse = openStream()) {
            List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promVectorResponse).getResults();
            assertThat(results.get(0).getMetricHeader().get("__name__")).isEqualTo("up");
        }
    }

    @Test
    public void verifyMetricTimestampResponse()
            throws IOException
    {
        try (InputStream promVectorResponse = openStream()) {
            List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promVectorResponse).getResults();
            assertThat(results.get(0).getTimeSeriesValues().getValues().get(0).getTimestamp()).isEqualTo(ofEpochMilli(1565889995668L));
        }
    }

    @Test
    public void verifyMetricValueResponse()
            throws IOException
    {
        try (InputStream promVectorResponse = openStream()) {
            List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promVectorResponse).getResults();
            assertThat(results.get(0).getTimeSeriesValues().getValues().get(0).getValue()).isEqualTo("1");
        }
    }

    private static InputStream openStream()
            throws IOException
    {
        URL promMatrixResponse = Resources.getResource(TestPrometheusQueryVectorResponseParse.class, "/prometheus-data/up_vector_response.json");
        assertThat(promMatrixResponse)
                .describedAs("metadataUrl is null")
                .isNotNull();
        return promMatrixResponse.openStream();
    }
}
