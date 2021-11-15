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

public class TestS3SecurityMappingsParser
{
    @Test
    public void testParse()
    {
        S3SecurityMappingConfig conf = new S3SecurityMappingConfig()
                .setJsonPointer("/data");

        S3SecurityMappingsParser provider = new S3SecurityMappingsParser(conf);
        S3SecurityMappings mappings =
                provider.parseJSONString("{\"data\": {\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}, \"time\": \"30s\"}");

        Optional<S3SecurityMapping> mapping = mappings.getMapping(ConnectorIdentity.ofUser("test"), URI.create("http://trino"));
        assertTrue(mapping.isPresent());
    }

    @Test
    public void testParseDefault()
    {
        S3SecurityMappingConfig conf = new S3SecurityMappingConfig();

        S3SecurityMappingsParser provider = new S3SecurityMappingsParser(conf);
        S3SecurityMappings mappings =
                provider.parseJSONString("{\"mappings\": [{\"iamRole\":\"arn:aws:iam::test\",\"user\":\"test\"}]}");

        Optional<S3SecurityMapping> mapping = mappings.getMapping(ConnectorIdentity.ofUser("test"), URI.create("http://trino"));
        assertTrue(mapping.isPresent());
    }
}
