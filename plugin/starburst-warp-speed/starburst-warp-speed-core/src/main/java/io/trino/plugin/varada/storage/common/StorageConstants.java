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
package io.trino.plugin.varada.storage.common;

public final class StorageConstants
{
    public static final int NULL_VALUE_MARKER_SIZE = 1;
    public static final int NULL_EXISTS_MARKER_SIZE = 1;
    public static final int DONT_HAVE_NULL = 0;
    public static final int MAY_HAVE_NULL = 1;
    public static final int NOT_NULL_MARKER_VALUE = 0;
    public static final int NULL_MARKER_VALUE = 1;
    public static final int INDEX_PADDING_SIZE = 32;

    private StorageConstants()
    {
    }
}
