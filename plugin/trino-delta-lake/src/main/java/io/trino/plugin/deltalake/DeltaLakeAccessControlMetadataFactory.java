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
package io.trino.plugin.deltalake;

import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.security.AccessControlMetadata;

public interface DeltaLakeAccessControlMetadataFactory
{
    DeltaLakeAccessControlMetadataFactory SYSTEM = metastore -> new AccessControlMetadata() {
        @Override
        public boolean isUsingSystemSecurity()
        {
            return true;
        }
    };
    DeltaLakeAccessControlMetadataFactory DEFAULT = metastore -> new AccessControlMetadata() {};

    AccessControlMetadata create(HiveMetastore metastore);
}
