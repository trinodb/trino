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
package io.trino.tests.product.launcher.util;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public final class DirectoryUtils
{
    private DirectoryUtils() {}

    public static List<Path> listDirectDescendants(Path directory)
    {
        return Stream.of(requireNonNull(directory.toFile().listFiles(), "listFiles is null"))
                .map(file -> directory.resolve(file.getName()))
                .collect(toImmutableList());
    }

    public static Path getOnlyDescendant(Path directory)
    {
        return getOnlyElement(listDirectDescendants(directory));
    }
}
