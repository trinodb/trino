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
package io.prestosql.tests.product.launcher.env;

public final class EnvironmentContainers
{
    public static final String PRESTO = "presto";
    public static final String COORDINATOR = PRESTO + "-master";
    public static final String WORKER = PRESTO + "-worker";
    public static final String WORKER_NTH = WORKER + "-";
    public static final String HADOOP = "hadoop-master";
    public static final String TESTS = "tests";
    public static final String LDAP = "ldapserver";

    private EnvironmentContainers() {}

    public static String worker(int number)
    {
        return WORKER_NTH + number;
    }

    public static boolean isPrestoContainer(String name)
    {
        return name.startsWith(PRESTO);
    }
}
