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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import static java.time.Instant.ofEpochMilli;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@TestInstance(PER_METHOD)
@Execution(SAME_THREAD)
public class TestPrometheusQueryMatrixResponseParse
{
    private InputStream promMatrixResponse;
    private InputStream promErrorResponse;

    @Test
    public void trueStatusOnSuccessResponse()
            throws IOException
    {
        assertThat(new PrometheusQueryResponseParse(promMatrixResponse).getStatus()).isTrue();
    }

    @Test
    public void verifyMetricPropertiesResponse()
            throws IOException
    {
        List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promMatrixResponse).getResults();
        assertThat(results.get(0).getMetricHeader()).containsEntry("__name__", "up");
    }

    @Test
    public void verifyMetricTimestampResponse()
            throws IOException
    {
        List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promMatrixResponse).getResults();
        assertThat(results.get(0).getTimeSeriesValues().getValues().get(0).getTimestamp()).isEqualTo(ofEpochMilli(1565962969044L));
    }

    @Test
    public void verifyMetricValueResponse()
            throws IOException
    {
        List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promMatrixResponse).getResults();
        assertThat(results.get(0).getTimeSeriesValues().getValues().get(0).getValue()).isEqualTo("1");
    }

    @Test
    public void verifyOnErrorResponse()
    {
        assertThatThrownBy(() -> new PrometheusQueryResponseParse(promErrorResponse))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Unable to parse Prometheus response: error bad_data invalid parameter 'query': parse error at char 4: bad duration syntax");
    }

    @BeforeEach
    public void setUp()
            throws Exception
    {
        URL promMatrixResponse = Resources.getResource(getClass(), "/prometheus-data/up_matrix_response.json");
        assertThat(promMatrixResponse)
                .describedAs("metadataUrl is null")
                .isNotNull();
        this.promMatrixResponse = promMatrixResponse.openStream();

        URL promErrorResponse = Resources.getResource(getClass(), "/prometheus-data/prom_error_response.json");
        assertThat(promMatrixResponse)
                .describedAs("metadataUrl is null")
                .isNotNull();
        this.promErrorResponse = promErrorResponse.openStream();
    }

    @AfterEach
    public void tearDown()
            throws Exception
    {
        promMatrixResponse.close();
        promMatrixResponse = null;
        promErrorResponse.close();
        promErrorResponse = null;
    }
}
