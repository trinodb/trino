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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.plugin.prometheus.PrometheusErrorCode.PROMETHEUS_PARSE_ERROR;
import static java.util.Collections.singletonList;

public class PrometheusQueryResponseParse
{
    private boolean status;
    private String resultType;
    private String result;
    private List<PrometheusMetricResult> results;

    public PrometheusQueryResponseParse(InputStream response)
            throws IOException
    {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        JsonParser parser = jsonFactory().createParser(response);
        while (!parser.isClosed()) {
            JsonToken jsonToken = parser.nextToken();
            if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                if (parser.currentName().equals("status")) {
                    parser.nextToken();
                    if (parser.getValueAsString().equals("success")) {
                        this.status = true;
                        while (!parser.isClosed()) {
                            parser.nextToken();
                            if (JsonToken.FIELD_NAME.equals(jsonToken)) {
                                if (parser.currentName().equals("resultType")) {
                                    parser.nextToken();
                                    resultType = parser.getValueAsString();
                                }
                                if (parser.currentName().equals("result")) {
                                    parser.nextToken();
                                    ArrayNode node = mapper.readTree(parser);
                                    result = node.toString();
                                    break;
                                }
                            }
                        }
                    }
                    else {
                        //error path
                        String parsedStatus = parser.getValueAsString();
                        parser.nextToken();
                        parser.nextToken();
                        String errorType = parser.getValueAsString();
                        parser.nextToken();
                        parser.nextToken();
                        String error = parser.getValueAsString();
                        throw new TrinoException(PROMETHEUS_PARSE_ERROR, "Unable to parse Prometheus response: " + parsedStatus + " " + errorType + " " + error);
                    }
                }
            }
            if (result != null) {
                break;
            }
        }
        if (result != null && resultType != null) {
            switch (resultType) {
                case "matrix":
                case "vector":
                    results = mapper.readValue(result, new TypeReference<List<PrometheusMetricResult>>() {});
                    break;
                case "scalar":
                case "string":
                    PrometheusTimeSeriesValue stringOrScalarResult = mapper.readValue(result, new TypeReference<PrometheusTimeSeriesValue>() {});
                    Map<String, String> madeUpMetricHeader = new HashMap<>();
                    madeUpMetricHeader.put("__name__", resultType);
                    PrometheusTimeSeriesValueArray timeSeriesValues = new PrometheusTimeSeriesValueArray(singletonList(stringOrScalarResult));
                    results = singletonList(new PrometheusMetricResult(madeUpMetricHeader, timeSeriesValues));
            }
        }
    }

    public List<PrometheusMetricResult> getResults()
    {
        return results;
    }

    public boolean getStatus()
    {
        return this.status;
    }
}
