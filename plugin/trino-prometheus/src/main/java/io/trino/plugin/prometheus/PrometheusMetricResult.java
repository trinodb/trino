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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrometheusMetricResult
{
    private final Map<String, String> metricHeader;
    private final PrometheusTimeSeriesValueArray timeSeriesValues;

    @JsonCreator
    public PrometheusMetricResult(
            @JsonProperty("metric") Map<String, String> metricHeader,
            @JsonProperty("values") PrometheusTimeSeriesValueArray timeSeriesValues,
            @JsonProperty("value") PrometheusTimeSeriesValue timeSeriesValue)
    {
        requireNonNull(metricHeader, "metricHeader is null");
        this.metricHeader = metricHeader;
        if (timeSeriesValues != null) {
            this.timeSeriesValues = timeSeriesValues;
        }
        else if (timeSeriesValue != null) {
            this.timeSeriesValues = new PrometheusTimeSeriesValueArray(ImmutableList.of(timeSeriesValue));
        }
        else {
            this.timeSeriesValues = null;
        }
    }

    public Map<String, String> getMetricHeader()
    {
        return metricHeader;
    }

    @JsonProperty("values")
    public PrometheusTimeSeriesValueArray getTimeSeriesValues()
    {
        return timeSeriesValues;
    }
}
