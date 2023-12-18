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
package io.trino.plugin.opa;

import com.google.common.collect.ImmutableSet;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.trino.execution.QueryIdGenerator;
import io.trino.spi.security.Identity;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.security.SystemSecurityContext;

import java.net.URI;
import java.time.Instant;

public final class TestConstants
{
    private TestConstants() {}

    public static final HttpClientUtils.MockResponse OK_RESPONSE = new HttpClientUtils.MockResponse("""
            {
                "decision_id": "",
                "result": true
            }
            """,
            200);
    public static final HttpClientUtils.MockResponse NO_ACCESS_RESPONSE = new HttpClientUtils.MockResponse("""
            {
                "decision_id": "",
                "result": false
            }
            """,
            200);
    public static final HttpClientUtils.MockResponse MALFORMED_RESPONSE = new HttpClientUtils.MockResponse("""
            { "this"": is broken_json; }
            """,
            200);
    public static final HttpClientUtils.MockResponse UNDEFINED_RESPONSE = new HttpClientUtils.MockResponse("{}", 404);
    public static final HttpClientUtils.MockResponse BAD_REQUEST_RESPONSE = new HttpClientUtils.MockResponse("{}", 400);
    public static final HttpClientUtils.MockResponse SERVER_ERROR_RESPONSE = new HttpClientUtils.MockResponse("", 500);
    public static final SystemAccessControlFactory.SystemAccessControlContext SYSTEM_ACCESS_CONTROL_CONTEXT = new TestingSystemAccessControlContext("TEST_VERSION");
    public static final URI OPA_SERVER_URI = URI.create("http://my-uri/");
    public static final URI OPA_SERVER_BATCH_URI = URI.create("http://my-batch-uri/");
    public static final URI OPA_ROW_FILTERING_URI = URI.create("http://my-row-filtering-uri/");
    public static final URI OPA_COLUMN_MASKING_URI = URI.create("http://my-column-masking-uri/");
    public static final Identity TEST_IDENTITY = Identity.forUser("source-user").withGroups(ImmutableSet.of("some-group")).build();
    public static final SystemSecurityContext TEST_SECURITY_CONTEXT = new SystemSecurityContext(TEST_IDENTITY, new QueryIdGenerator().createNextQueryId(), Instant.now());

    public static OpaConfig simpleOpaConfig()
    {
        return new OpaConfig().setOpaUri(OPA_SERVER_URI);
    }

    public static OpaConfig batchFilteringOpaConfig()
    {
        return simpleOpaConfig().setOpaBatchUri(OPA_SERVER_BATCH_URI);
    }

    public static OpaConfig rowFilteringOpaConfig()
    {
        return simpleOpaConfig().setOpaRowFiltersUri(OPA_ROW_FILTERING_URI);
    }

    public static OpaConfig columnMaskingOpaConfig()
    {
        return simpleOpaConfig().setOpaColumnMaskingUri(OPA_COLUMN_MASKING_URI);
    }

    static final class TestingSystemAccessControlContext
            implements SystemAccessControlFactory.SystemAccessControlContext
    {
        private final String trinoVersion;

        public TestingSystemAccessControlContext(String version)
        {
            this.trinoVersion = version;
        }

        @Override
        public String getVersion()
        {
            return this.trinoVersion;
        }

        @Override
        public OpenTelemetry getOpenTelemetry()
        {
            return null;
        }

        @Override
        public Tracer getTracer()
        {
            return null;
        }
    }
}
