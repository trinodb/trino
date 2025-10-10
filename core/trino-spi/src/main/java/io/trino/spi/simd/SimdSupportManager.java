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
package io.trino.spi.simd;

import io.trino.spi.SimdSupport;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Locale.ENGLISH;

/**
 * A lightweight, dependency-free holder for the engine-wide {@link SimdSupport}.
 *
 * This lives in SPI so both engine and plugins share the same static state.
 * Initialization should be triggered by the engine at startup (see trino-main),
 * but this class is safe to call lazily from anywhere.
 */
public final class SimdSupportManager
{
    private static final AtomicBoolean initialized = new AtomicBoolean(false);
    private static volatile SimdSupport current = SimdSupport.NONE;

    private SimdSupportManager() {}

    /**
     * Initialize the global SIMD support exactly once.
     */
    public static void initialize()
    {
        if (!initialized.compareAndSet(false, true)) {
            return;
        }
        current = detectSimd();
    }

    /**
     * Returns the current {@link SimdSupport}. If initialization hasn't occurred,
     * returns {@link SimdSupport#NONE}.
     */
    public static SimdSupport get()
    {
        if (!initialized.get()) {
            initialize();
        }
        return current;
    }

    /**
     * Returns whether initialization has occurred.
     */
    public static boolean isInitialized()
    {
        return initialized.get();
    }

    private static SimdSupport detectSimd()
    {
        OSType osType = detectOS();
        TargetArch targetArch = detectArch(osType);
        if (targetArch == TargetArch.X86_INTEL) {
            return new IntelSimdSupport(osType);
        }
        if (targetArch == TargetArch.X86_AMD) {
            return new AmdSimdSupport(osType);
        }
        if (targetArch == TargetArch.ARM_GRAVITON) {
            return new GravitonSimdSupport(osType);
        }
        return SimdSupport.NONE;
    }

    private static TargetArch detectArch(OSType os)
    {
        String arch = System.getProperty("os.arch", "").toLowerCase(ENGLISH);

        if (arch.contains("x86") || arch.contains("amd64") || arch.contains("x86_64")) {
            if (os == OSType.LINUX) {
                Optional<String> vendor = SimdUtils.linuxCpuVendorId();
                if (vendor.isPresent()) {
                    String v = vendor.get();
                    if (v.contains("intel")) {
                        return TargetArch.X86_INTEL;
                    }
                    if (v.contains("amd")) {
                        return TargetArch.X86_AMD;
                    }
                }
            }
        }

        if (arch.contains("aarch64") || arch.contains("arm64")) {
            if (SimdUtils.procCpuInfoContains("graviton")) {
                return TargetArch.ARM_GRAVITON;
            }
        }

        return TargetArch.Other;
    }

    private static OSType detectOS()
    {
        String os = System.getProperty("os.name", "").toLowerCase(ENGLISH);
        if (os.contains("linux")) {
            return OSType.LINUX;
        }
        return OSType.Other;
    }
}
