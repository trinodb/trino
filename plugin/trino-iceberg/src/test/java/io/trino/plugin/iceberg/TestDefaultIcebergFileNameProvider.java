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
package io.trino.plugin.iceberg;

import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.apache.iceberg.FileContent.DATA;
import static org.apache.iceberg.FileContent.POSITION_DELETES;
import static org.apache.iceberg.FileFormat.AVRO;
import static org.apache.iceberg.FileFormat.ORC;
import static org.apache.iceberg.FileFormat.PARQUET;
import static org.apache.iceberg.FileFormat.PUFFIN;
import static org.assertj.core.api.Assertions.assertThat;

final class TestDefaultIcebergFileNameProvider
{
    private static final String UUID_PATTERN = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}";

    @Test
    void testFileNames()
    {
        IcebergFileNameProvider provider = new DefaultIcebergFileNameProvider();
        String prefix = SESSION.getQueryId() + "-" + UUID_PATTERN;

        assertThat(provider.newFileName(SESSION, DATA, PARQUET)).matches(prefix + "\\.parquet");
        assertThat(provider.newFileName(SESSION, DATA, ORC)).matches(prefix + "\\.orc");
        assertThat(provider.newFileName(SESSION, DATA, AVRO)).matches(prefix + "\\.avro");
        assertThat(provider.newFileName(SESSION, POSITION_DELETES, PARQUET)).matches(prefix + "\\.parquet");
        assertThat(provider.newFileName(SESSION, POSITION_DELETES, PUFFIN)).matches("dv-" + prefix + "\\.puffin");
        assertThat(provider.newSortTempFilePrefix(SESSION)).matches("sorting-file-writer-" + prefix);

        assertThat(provider.newFileName(SESSION, DATA, PARQUET)).isNotEqualTo(provider.newFileName(SESSION, DATA, PARQUET));
        assertThat(provider.newSortTempFilePrefix(SESSION)).isNotEqualTo(provider.newSortTempFilePrefix(SESSION));
    }
}
