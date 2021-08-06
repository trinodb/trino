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
package io.trino.plugin.fission;

import io.airlift.slice.Slice;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;

public class FissionFunctionFunctions
{
    private static final BasicCookieStore cookieStore = new BasicCookieStore();

    private static String baseUrl = "";

    private FissionFunctionFunctions()
    {
    }

    @ScalarFunction("fission_dnsdb")
    @Description("Converts the string to alternating case")
    @SqlType(StandardTypes.BIGINT)
    public static long dnsdb(@SqlType(StandardTypes.VARCHAR) Slice slice) throws JSONException, IOException
    {
        CloseableHttpClient httpClient;
        HttpClientBuilder builder = HttpClientBuilder.create().setDefaultCookieStore(cookieStore);
        httpClient = builder.build();
        String result = "";
        int count = 0;
        try {
            HttpGet getRequest = new HttpGet(String.format("%s/dnsdb?lookup=%s", FissionFunctionConfigProvider.getFissionFunctionBaseURL(), slice.toStringUtf8()));
            HttpResponse response = httpClient.execute(getRequest);
            result = EntityUtils.toString(response.getEntity());
            JSONObject myObject = new JSONObject(result);

            if (myObject.has("Message")) {
                count = 0;
            }
            else if (myObject.has("total_count")) {
                count = Integer.parseInt(myObject.getString("total_count"));
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (JSONException e) {
            throw e;
        }
        return count;
    }

    @ScalarFunction("fission_despicablename")
    @Description("Converts the string to alternating case")
    @SqlType(StandardTypes.BIGINT)
    public static long despicablename(@SqlType(StandardTypes.VARCHAR) Slice slice) throws JSONException, IOException
    {
        CloseableHttpClient httpClient;
        HttpClientBuilder builder = HttpClientBuilder.create().setDefaultCookieStore(cookieStore);
        httpClient = builder.build();
        String result = "";
        int count = 0;
        try {
            HttpGet getRequest = new HttpGet(String.format("%s/despicablename?domain=%s", FissionFunctionConfigProvider.getFissionFunctionBaseURL(), slice.toStringUtf8()));
            HttpResponse response = httpClient.execute(getRequest);
            result = EntityUtils.toString(response.getEntity());
            JSONObject myObject = new JSONObject(result);

            if (myObject.has("probs")) {
                count = Integer.parseInt(myObject.getJSONArray("probs").getJSONArray(0).getString(0));
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (JSONException e) {
            throw e;
        }
        return count;
    }
}
