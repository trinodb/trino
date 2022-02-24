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
package io.trino.tests.product.launcher.env;

import java.util.List;

public enum SupportedTrinoJdk
{
    ZULU_11("/usr/lib/jvm/zulu-11", List.of(
        "-Xmx2G",
        "-XX:+UseG1GC",
        "-XX:-UseBiasedLocking",
        "-XX:G1HeapRegionSize=32M",
        // Force Parallel GC to ensure MaxHeapFreeRatio is respected
        "-XX:+UseParallelGC",
        "-XX:MinHeapFreeRatio=10",
        "-XX:MaxHeapFreeRatio=10")),
    ZULU_17("/usr/lib/jvm/zulu-17", List.of(
        "-Xmx2304M",
        "-XX:+UseZGC",
        "-Dzookeeper.sasl.client.canonicalize.hostname=false",
        "-Dzookeeper.sasl.client=false",
        "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens", "java.base/java.nio=ALL-UNNAMED")),
    /**/;

    private final String javaHome;
    private final List<String> jvmFlags;

    SupportedTrinoJdk(String javaHome, List<String> jvmFlags)
    {
        this.javaHome = javaHome;
        this.jvmFlags = jvmFlags;
    }

    public String getJavaHome()
    {
        return javaHome;
    }

    public String getJavaCommand()
    {
        return javaHome + "/bin/java";
    }

    public List<String> getJvmFlags()
    {
        return jvmFlags;
    }
}
