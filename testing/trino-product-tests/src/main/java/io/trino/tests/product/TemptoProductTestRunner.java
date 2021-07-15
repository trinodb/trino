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
package io.trino.tests.product;

import io.trino.tempto.runner.TemptoRunner;
import io.trino.tempto.runner.TemptoRunnerCommandLineParser;

public final class TemptoProductTestRunner
{
    public static void main(String[] args)
    {
        TemptoRunnerCommandLineParser parser = TemptoRunnerCommandLineParser.builder("Trino product tests")
                .setTestsPackage("io.trino.tests.product.*", false)
                .setExcludedGroups("quarantine", true)
                .build();
        TemptoRunner.runTempto(parser, args);

        // Some libraries (e.g. apparently Datastax's Cassandra driver) can start non-daemon threads.
        // Explicit exit() is required to terminate test process.
        System.exit(0);
    }

    private TemptoProductTestRunner() {}
}
