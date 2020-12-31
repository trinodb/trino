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
package io.prestosql.tests.product.launcher.cli;

import com.google.common.base.Strings;
import picocli.CommandLine;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public class OptionalPathConverter
        implements CommandLine.ITypeConverter<Optional<Path>>
{
    @Override
    public Optional<Path> convert(String value)
    {
        if (Strings.isNullOrEmpty(value)) {
            return Optional.empty();
        }

        return Optional.of(Paths.get(value));
    }
}
