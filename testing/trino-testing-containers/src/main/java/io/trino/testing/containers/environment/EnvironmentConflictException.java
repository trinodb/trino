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
package io.trino.testing.containers.environment;

/**
 * Thrown when a test requests a different environment than the one currently running,
 * and the EnvironmentManager is in STRICT mode.
 */
public class EnvironmentConflictException
        extends RuntimeException
{
    @SuppressWarnings("AnnotateFormatMethod")
    public EnvironmentConflictException(String message, Object... args)
    {
        super(String.format(message, args));
    }
}
