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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;

import static java.time.Instant.ofEpochMilli;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true) // see @BeforeMethod
public class TestPrometheusQueryVectorResponseParse
{
    private InputStream promVectorResponse;

    @Test
    public void trueStatusOnSuccessResponse()
            throws IOException
    {
        assertTrue(new PrometheusQueryResponseParse(promVectorResponse).getStatus());
    }

    @Test
    public void verifyMetricPropertiesResponse()
            throws IOException
    {
        List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promVectorResponse).getResults();
        assertEquals(results.get(0).getMetricHeader().get("__name__"), "up");
    }

    @Test
    public void verifyMetricTimestampResponse()
            throws IOException
    {
        List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promVectorResponse).getResults();
        assertEquals(results.get(0).getTimeSeriesValues().getValues().get(0).getTimestamp(), ofEpochMilli(1565889995668L));
    }

    @Test
    public void verifyMetricValueResponse()
            throws IOException
    {
        List<PrometheusMetricResult> results = new PrometheusQueryResponseParse(promVectorResponse).getResults();
        assertEquals(results.get(0).getTimeSeriesValues().getValues().get(0).getValue(), "1");
    }

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        URL promMatrixResponse = Resources.getResource(getClass(), "/prometheus-data/up_vector_response.json");
        assertNotNull(promMatrixResponse, "metadataUrl is null");
        this.promVectorResponse = promMatrixResponse.openStream();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        promVectorResponse.close();
        promVectorResponse = null;
    }
}
