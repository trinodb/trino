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
package io.prestosql.dispatcher;

import io.airlift.jaxrs.testing.MockUriInfo;
import org.testng.annotations.Test;

import javax.ws.rs.core.UriInfo;

import static io.prestosql.dispatcher.QueuedStatementResource.getScheme;
import static org.testng.Assert.assertEquals;

public class TestQueuedStatementResource
{
    @Test
    public void testGetScheme()
    {
        UriInfo uriInfo = MockUriInfo.from("http://example.com");
        assertEquals(getScheme("https", uriInfo), "https");
        assertEquals(getScheme("https,http", uriInfo), "https");
        assertEquals(getScheme("", uriInfo), "http");
        assertEquals(getScheme(null, uriInfo), "http");
        assertEquals(getScheme(",https", uriInfo), "https");
    }
}
