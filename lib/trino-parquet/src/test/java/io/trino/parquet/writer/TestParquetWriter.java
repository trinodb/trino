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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class TestParquetWriter
{
    @Test
    public void testCreatedByIsParsable()
            throws VersionParser.VersionParseException, IOException
    {
        String createdBy = ParquetWriter.formatCreatedBy("test-version");
        VersionParser.ParsedVersion version = VersionParser.parse(createdBy);
        assertThat(version).isNotNull();
        assertThat(version.application).isEqualTo("Trino");
        assertThat(version.version).isEqualTo("test-version");
        assertThat(version.appBuildHash).isEqualTo("n/a");

        // Ensure that createdBy field is parsable in CDH 5 to avoid the exception "parquet.io.ParquetDecodingException: Cannot read data due to PARQUET-246: to read safely, set parquet.split.files to false";
        // the pattern is taken from https://github.com/cloudera/parquet-mr/blob/cdh5-1.5.0_5.15.1/parquet-common/src/main/java/parquet/VersionParser.java#L34
        Pattern pattern = Pattern.compile("(.+) version ((.*) )?\\(build ?(.*)\\)");
        Matcher matcher = pattern.matcher(createdBy);
        assertThat(matcher.matches()).isTrue();
        assertThat(matcher.group(1)).isEqualTo("Trino");
        assertThat(matcher.group(3)).isEqualTo("test-version");
        assertThat(matcher.group(4)).isEqualTo("n/a");
    }
}
