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
package io.trino.filesystem.s3;

import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static com.google.common.net.MediaType.JSON_UTF_8;
import static io.airlift.http.client.testing.TestingResponse.mockResponse;
import static org.assertj.core.api.Assertions.assertThat;

public class TestS3SecurityMappingsUriSource
{
    private static final String MOCK_MAPPINGS_RESPONSE =
            "{\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}";

    @Test
    public void testGetRawJson()
    {
        Response response = mockResponse(HttpStatus.OK, JSON_UTF_8, MOCK_MAPPINGS_RESPONSE);
        S3SecurityMappingConfig config = new S3SecurityMappingConfig().setConfigUri(URI.create("http://test:1234/api/endpoint"));
        var provider = new S3SecurityMappingsUriSource(config, new TestingHttpClient(_ -> response));
        String result = provider.getRawJsonString();
        assertThat(result).isEqualTo(MOCK_MAPPINGS_RESPONSE);
    }
}
