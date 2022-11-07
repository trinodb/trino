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
package io.trino.parquet.writer;

import org.apache.parquet.VersionParser;
import org.testng.annotations.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetWriter
{
    @Test
    public void testCreatedByIsParsable()
            throws VersionParser.VersionParseException, IOException
    {
        String createdBy = ParquetWriter.formatCreatedBy("test-version");
        // createdBy must start with "parquet-mr" to make Apache Hive perform timezone conversion on INT96 timestamps correctly
        // when hive.parquet.timestamp.skip.conversion is set to true.
        // Apache Hive 3.2 and above enable hive.parquet.timestamp.skip.conversion by default
        assertThat(createdBy).startsWith("parquet-mr");
        VersionParser.ParsedVersion version = VersionParser.parse(createdBy);
        assertThat(version).isNotNull();
        assertThat(version.application).isEqualTo("parquet-mr-trino");
        assertThat(version.version).isEqualTo("test-version");
        assertThat(version.appBuildHash).isEqualTo("n/a");
    }
}
