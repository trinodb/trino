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
package io.trino.spi.filesystem;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Class that holds constant values for supported file attributes
 * To add new attributes, make sure it is supported by both local and hdfs filesystem
 * It is recommended to use the string literal of supported properties from java.nio.file.attribute package
 * to maximize compatibility.
 * <p>
 * When new attributes are added change the getAttribute() methods to cover the newly added one accordingly.
 * The return type of the attribute need to be documented in
 * {@link HetuFileSystemClient#getAttribute(Path, String)}'s java doc.
 * Ensure they have same return type across all filesystems. If not convert them.
 * <p>
 * Tests on the return type should be added in the {@code testGetAttribute()} in all client tests.
 *
 * @since 2020-04-02
 */
public class SupportedFileAttributes
{
    public static final String LAST_MODIFIED_TIME = "lastModifiedTime";
    public static final String SIZE = "size";
    public static final Set SUPPORTED_ATTRIBUTES = ImmutableSet.of(
            LAST_MODIFIED_TIME,
            SIZE);

    private SupportedFileAttributes()
    {
    }
}
