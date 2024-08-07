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

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;

public class QueryResult
{
    static ObjectMapper mapper = new ObjectMapper();

    public static QueryResult fromJSON(InputStream input)
            throws IOException
    {
        return mapper.readValue(input, QueryResult.class);
    }

    public String getStatus()
    {
        return status;
    }

    public Data getData()
    {
        return data;
    }

    public void setStatus(String status)
    {
        this.status = status;
    }

    public void setData(Data data)
    {
        this.data = data;
    }

    private String status;
    private Data data;

    public abstract static sealed class Result
            permits Streams, Matrix {}
}
