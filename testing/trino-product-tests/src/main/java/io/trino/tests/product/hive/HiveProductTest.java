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
package io.trino.tests.product.hive;

import com.google.common.base.Throwables;
import io.airlift.log.Logger;
import io.trino.tempto.ProductTest;
import io.trino.tempto.query.QueryResult;
import net.jodah.failsafe.RetryPolicy;
import org.intellij.lang.annotations.Language;

import javax.inject.Inject;

import java.time.temporal.ChronoUnit;
import java.util.regex.Pattern;

public class HiveProductTest
        extends ProductTest
{
    private static final Logger log = Logger.get(HiveProductTest.class);

    static final String ERROR_COMMITTING_WRITE_TO_HIVE_ISSUE = "https://github.com/trinodb/trino/issues/4936";
    @Language("RegExp")
    static final String ERROR_COMMITTING_WRITE_TO_HIVE_MATCH =
            // "Error committing write to Hive" is present depending on when the exception is thrown.
            // It may be absent when the underlying problem manifest earlier (e.g. during RecordFileWriter.appendRow vs RecordFileWriter.commit).

            // "could only be written to 0 of the 1 minReplication" is the error wording used by e.g. HDP 3
            "(could only be replicated to 0 nodes instead of minReplication|could only be written to 0 of the 1 minReplication)";

    public static final RetryPolicy<QueryResult> ERROR_COMMITTING_WRITE_TO_HIVE_RETRY_POLICY = new RetryPolicy<QueryResult>()
            .handleIf(HiveProductTest::isErrorCommittingToHive)
            .withBackoff(1, 10, ChronoUnit.SECONDS)
            .withMaxRetries(30)
            .onRetry(event -> log.warn(event.getLastFailure(), "Query failed on attempt %d, will retry.", event.getAttemptCount()));

    private static boolean isErrorCommittingToHive(Throwable throwable)
    {
        return Pattern.compile(ERROR_COMMITTING_WRITE_TO_HIVE_MATCH)
                .matcher(Throwables.getStackTraceAsString(throwable))
                .find();
    }

    @Inject
    private HiveVersionProvider hiveVersionProvider;

    protected int getHiveVersionMajor()
    {
        return hiveVersionProvider.getHiveVersion().getMajorVersion();
    }

    protected int getHiveVersionMinor()
    {
        return hiveVersionProvider.getHiveVersion().getMinorVersion();
    }

    protected int getHiveVersionPatch()
    {
        return hiveVersionProvider.getHiveVersion().getPatchVersion();
    }

    protected boolean isHiveVersionBefore12()
    {
        return getHiveVersionMajor() == 0
                || (getHiveVersionMajor() == 1 && getHiveVersionMinor() < 2);
    }

    protected boolean isHiveWithBrokenAvroTimestamps()
    {
        // In 3.1.0 timestamp semantics in hive changed in backward incompatible way,
        // which was fixed for Parquet and Avro in 3.1.2 (https://issues.apache.org/jira/browse/HIVE-21002)
        // we do have a work-around for Parquet, but still need this for Avro until
        // https://github.com/trinodb/trino/issues/5144 is addressed
        return getHiveVersionMajor() == 3 &&
                getHiveVersionMinor() == 1 &&
                (getHiveVersionPatch() == 0 || getHiveVersionPatch() == 1);
    }
}
