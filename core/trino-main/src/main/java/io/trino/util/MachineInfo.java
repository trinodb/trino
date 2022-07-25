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
package io.trino.util;

import com.google.common.base.StandardSystemProperty;
import oshi.SystemInfo;

import static java.lang.Math.min;

public final class MachineInfo
{
    // cache physical processor count, so that it's not queried multiple times during tests
    private static volatile int physicalProcessorCount = -1;

    private MachineInfo() {}

    public static int getAvailablePhysicalProcessorCount()
    {
        if (physicalProcessorCount != -1) {
            return physicalProcessorCount;
        }

        String osArch = StandardSystemProperty.OS_ARCH.value();
        // logical core count (including container cpu quota if there is any)
        int availableProcessorCount = Runtime.getRuntime().availableProcessors();
        int totalPhysicalProcessorCount;
        if ("amd64".equals(osArch) || "x86_64".equals(osArch)) {
            // Oshi can recognize physical processor count (without hyper threading) for x86 platforms.
            // However, it doesn't correctly recognize physical processor count for ARM platforms.
            totalPhysicalProcessorCount = new SystemInfo()
                    .getHardware()
                    .getProcessor()
                    .getPhysicalProcessorCount();
        }
        else {
            // ARM platforms do not support hyper threading, therefore each logical processor is separate core
            totalPhysicalProcessorCount = availableProcessorCount;
        }

        // cap available processor count to container cpu quota (if there is any).
        physicalProcessorCount = min(totalPhysicalProcessorCount, availableProcessorCount);
        return physicalProcessorCount;
    }
}
