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
package io.trino.tests.product.launcher;

import com.google.inject.Module;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static java.util.Objects.requireNonNull;

public final class Extensions
{
    private final Module additionalEnvironments;
    private final Module additionalSuites;

    public Extensions(Module additionalEnvironments)
    {
        this(additionalEnvironments, EMPTY_MODULE);
    }

    public Extensions(Module additionalEnvironments, Module additionalSuites)
    {
        this.additionalEnvironments = requireNonNull(additionalEnvironments, "additionalEnvironments is null");
        this.additionalSuites = requireNonNull(additionalSuites, "additionalSuites is null");
    }

    public Module getAdditionalEnvironments()
    {
        return additionalEnvironments;
    }

    public Module getAdditionalSuites()
    {
        return additionalSuites;
    }
}
