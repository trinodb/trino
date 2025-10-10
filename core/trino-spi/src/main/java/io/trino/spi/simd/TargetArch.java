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

/**
 * Enumerates target CPU feature levels or instance families we may gate SIMD usage on.
 * Note: Detection is best-effort from public JVM signals; results are cached at startup.
 */
public enum TargetArch {
    X86_INTEL,
    X86_AMD,
    ARM_GRAVITON,
    Other
}
