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
package io.trino.plugin.iceberg.catalog.hms;

import io.trino.hive.thrift.metastore.MetaException;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.iceberg.catalog.hms.HiveMetastoreTableOperations.isConcurrentModificationRejection;
import static org.assertj.core.api.Assertions.assertThat;

final class TestHiveMetastoreTableOperations
{
    @Test
    void testIsConcurrentModificationRejection()
    {
        assertThat(isConcurrentModificationRejection(metastoreException("The table has been modified. The parameter value for key 'metadata_location' is different"))).isTrue();
        assertThat(isConcurrentModificationRejection(metastoreException("The table has been modified. The parameter value for key 'metadata_location' is 's3://b/00002.metadata.json'. The expected was value was 's3://b/00001.metadata.json'"))).isTrue();

        assertThat(isConcurrentModificationRejection(new MetaException())).isFalse();
        assertThat(isConcurrentModificationRejection(metastoreException("The table has been modified. The parameter value for key 'comment' is different"))).isFalse();
    }

    private static RuntimeException metastoreException(String message)
    {
        return new TrinoException(HIVE_METASTORE_ERROR, new MetaException(message));
    }
}
