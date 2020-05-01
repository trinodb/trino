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
package io.prestosql.tests.product.launcher;

import com.google.common.base.Suppliers;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public final class PathResolver
{
    public static final String PROJECT_VERSION_PLACEHOLDER = "${project.version}";

    private final Supplier<String> projectVersion = Suppliers.memoize(this::readProjectVersion);

    public File resolvePlaceholders(File path)
    {
        String result = path.getPath();
        if (result.contains(PROJECT_VERSION_PLACEHOLDER)) {
            result = result.replace(PROJECT_VERSION_PLACEHOLDER, projectVersion.get());
        }
        return new File(result);
    }

    private String readProjectVersion()
    {
        try {
            String version = Resources.toString(Resources.getResource("presto-product-tests-launcher-version.txt"), UTF_8).trim();
            checkState(!version.isEmpty(), "version is empty");
            return version;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
