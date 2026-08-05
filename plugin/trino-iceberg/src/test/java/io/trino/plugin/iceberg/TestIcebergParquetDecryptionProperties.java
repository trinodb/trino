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

import io.trino.parquet.crypto.FileDecryptionProperties;
import io.trino.plugin.iceberg.IcebergSplit.ParquetFileDecryptionData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.iceberg.IcebergPageSourceProvider.createParquetFileDecryptionProperties;
import static org.assertj.core.api.Assertions.assertThat;

final class TestIcebergParquetDecryptionProperties
{
    @Test
    void testParquetFileDecryptionProperties()
    {
        byte[] fileKey = new byte[] {1, 2, 3};
        byte[] aadPrefix = new byte[] {4, 5, 6};

        assertThat(createParquetFileDecryptionProperties(Optional.empty(), false)).isEmpty();

        Optional<FileDecryptionProperties> fileDecryptionProperties = createParquetFileDecryptionProperties(
                Optional.of(new ParquetFileDecryptionData(fileKey, aadPrefix)),
                false);
        assertThat(fileDecryptionProperties).isPresent();

        FileDecryptionProperties properties = fileDecryptionProperties.orElseThrow();
        assertThat(properties.getAadPrefix()).hasValue(aadPrefix);
        assertThat(properties.getKeyRetriever().getFooterKey(Optional.empty()))
                .hasValueSatisfying(key -> assertThat(key).containsExactly(fileKey));
        assertThat(properties.getKeyRetriever().getColumnKey(ColumnPath.fromDotString("c"), Optional.empty()))
                .hasValueSatisfying(key -> assertThat(key).containsExactly(fileKey));
    }
}
