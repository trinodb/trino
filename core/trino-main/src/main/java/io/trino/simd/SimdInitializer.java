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
package io.trino.simd;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.trino.spi.SimdSupport;
import io.trino.spi.simd.SimdSupportManager;

/**
 * Eagerly triggers SIMD initialization at server startup.
 */
@Singleton
public final class SimdInitializer
{
    private final SimdSupport simd;

    @Inject
    public SimdInitializer()
    {
        SimdSupportManager.initialize();
        this.simd = SimdSupportManager.get();
    }

    /**
     * Returns the detected SimdSupport after initialization.
     */
    public SimdSupport simdSupport()
    {
        return simd;
    }
}
