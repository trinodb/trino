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
package io.trino.loki.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Data
{
    public enum ResultType
    {
        @JsonProperty("streams") Streams,
        @JsonProperty("matrix") Matrix;
    }

    public ResultType getResultType()
    {
        return resultType;
    }

    public void setResultType(ResultType resultType)
    {
        this.resultType = resultType;
    }

    private ResultType resultType;

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "resultType", include = JsonTypeInfo.As.EXTERNAL_PROPERTY)
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(value = Streams.class, name = "streams"),
            @JsonSubTypes.Type(value = Matrix.class, name = "matrix")
    })
    private QueryResult.Result result;

    public QueryResult.Result getResult()
    {
        return result;
    }

    public void setResult(QueryResult.Result result)
    {
        this.result = result;
    }
}
