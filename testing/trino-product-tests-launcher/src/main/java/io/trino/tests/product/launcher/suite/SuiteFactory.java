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
package io.trino.tests.product.launcher.suite;

import com.google.common.collect.Ordering;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class SuiteFactory
{
    private final Map<String, Suite> suiteProviders;

    @Inject
    public SuiteFactory(Map<String, Suite> suiteProviders)
    {
        this.suiteProviders = requireNonNull(suiteProviders, "suiteProviders is null");
    }

    public Suite getSuite(String suiteName)
    {
        checkArgument(suiteProviders.containsKey(suiteName), "No suite with name '%s'. Those do exist, however: %s", suiteName, listSuites());
        return suiteProviders.get(suiteName);
    }

    public List<String> listSuites()
    {
        return Ordering.natural().sortedCopy(suiteProviders.keySet());
    }
}
