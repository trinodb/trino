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
package io.trino.plugin.iceberg.catalog.rest;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

import static io.trino.spi.testing.InterfaceTestUtils.assertAllMethodsOverridden;
import static io.trino.spi.testing.InterfaceTestUtils.assertProperForwardingMethodsAreCalled;

class TestSigV4AwsCredentialProvider
{
    @Test
    void testEverythingImplemented()
    {
        assertAllMethodsOverridden(AwsCredentialsProvider.class, SigV4AwsCredentialProvider.class);
    }

    @Test
    void testProperForwardingMethodsAreCalled()
    {
        assertProperForwardingMethodsAreCalled(AwsCredentialsProvider.class, SigV4AwsCredentialProvider::new);
    }
}
