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

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_TYPE;
import static org.apache.iceberg.CatalogProperties.ENCRYPTION_KMS_TYPE_AWS;

public class AwsKmsProperties
        implements KmsProperties
{
    @Override
    public Map<String, String> get()
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        properties.put(ENCRYPTION_KMS_TYPE, ENCRYPTION_KMS_TYPE_AWS);
        return properties.buildOrThrow();
    }
}
