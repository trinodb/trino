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
package io.trino.plugin.hive.s3;

import io.trino.spi.security.ConnectorIdentity;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Optional;

import static org.testng.Assert.assertTrue;

public class TestS3SecurityMappingsProvider
{
    @Test
    public void testParse()
    {
        S3SecurityMappingConfig conf = new S3SecurityMappingConfig()
                .setJsonPointer("/data");

        StubS3SecurityMappingsProvider provider = new StubS3SecurityMappingsProvider(conf);
        S3SecurityMappings mappings =
                provider.parse("{\"data\": {\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}, \"time\": \"30s\"}");

        Optional<S3SecurityMapping> mapping = mappings.getMapping(ConnectorIdentity.ofUser("test"), URI.create("http://trino"));
        assertTrue(mapping.isPresent());
    }

    @Test
    public void testParseDefault()
    {
        S3SecurityMappingConfig conf = new S3SecurityMappingConfig();

        StubS3SecurityMappingsProvider provider = new StubS3SecurityMappingsProvider(conf);
        S3SecurityMappings mappings =
                provider.parse("{\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}");

        Optional<S3SecurityMapping> mapping = mappings.getMapping(ConnectorIdentity.ofUser("test"), URI.create("http://trino"));
        assertTrue(mapping.isPresent());
    }

    public static class StubS3SecurityMappingsProvider
            extends S3SecurityMappingsProvider
    {
        public StubS3SecurityMappingsProvider(S3SecurityMappingConfig config)
        {
            super(config);
        }

        @Override
        protected String getRawJsonString()
        {
            return null;
        }
    }
}
