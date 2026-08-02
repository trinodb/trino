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
package io.trino.tests.product.utils;

import com.google.common.base.Throwables;
import org.intellij.lang.annotations.Language;

import java.util.regex.Pattern;

public final class HadoopTestUtils
{
    private HadoopTestUtils() {}

    /**
     * Link to issues:
     * <ol>
     *     <li><a href="https://github.com/trinodb/trino/issues/7535">#7535</a></li>
     *     <li><a href="https://github.com/trinodb/trino/issues/4936">#4936</a></li>
     *     <li><a href="https://github.com/trinodb/trino/issues/5427">#5427</a></li>
     * </ol>
     */
    public static final String RETRYABLE_FAILURES_ISSUES = "https://github.com/trinodb/trino/issues?q=is%3Aissue+issue%3A+4936+5427";
    @Language("RegExp")
    public static final String RETRYABLE_FAILURES_MATCH =
            // "Error committing write to Hive" is present depending on when the exception is thrown.
            // It may be absent when the underlying problem manifest earlier (e.g. during RecordFileWriter.appendRow vs RecordFileWriter.commit).
            "(could only be replicated to 0 nodes instead of minReplication" +
                    // "could only be written to 0 of the 1 minReplication" is the error wording used by e.g. HDP 3
                    "|could only be written to 0 of the 1 minReplication" +
                    // "Error while processing statement: FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.mr.MapRedTask. Error caching map.xml: java.nio.channels.ClosedByInterruptException"
                    "|return code [12] from \\Qorg.apache.hadoop.hive.ql.exec.mr.MapRedTask\\E" +
                    ")";

    public static boolean isErrorCommittingToHive(Throwable throwable)
    {
        return Pattern.compile(RETRYABLE_FAILURES_MATCH)
                .matcher(Throwables.getStackTraceAsString(throwable))
                .find();
    }
}
