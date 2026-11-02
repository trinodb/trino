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
package io.trino.plugin.opensearch;

import com.google.common.net.HostAndPort;
import org.apache.hc.core5.http.HttpHost;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;

import java.net.URISyntaxException;

public class RestClientUtils
{
    private RestClientUtils() {}

    @SuppressWarnings("deprecation")
    public static RestHighLevelClient createClient(HostAndPort address)
    {
        try {
            return new RestHighLevelClient(RestClient.builder(HttpHost.create(address.toString())));
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
