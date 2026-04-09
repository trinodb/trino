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
package io.trino.plugin.iceberg.encryption;

import org.apache.iceberg.encryption.KeyManagementClient;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

class TestKmsClientInstantiation
{
    @ParameterizedTest
    @ValueSource(strings = {"org.apache.iceberg.aws.AwsKeyManagementClient", "org.apache.iceberg.gcp.GcpKeyManagementClient"})
    void testKmsClientClassCanBeLoaded(String kmsImpl)
            throws Exception
    {
        Class<?> kmsClass = Class.forName(kmsImpl);
        assertThat(KeyManagementClient.class).isAssignableFrom(kmsClass);
        assertThat(kmsClass.getDeclaredConstructor().newInstance()).isInstanceOf(KeyManagementClient.class);
    }
}
