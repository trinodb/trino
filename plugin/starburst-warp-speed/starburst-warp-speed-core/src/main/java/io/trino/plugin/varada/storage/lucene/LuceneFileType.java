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
package io.trino.plugin.varada.storage.lucene;

import java.util.Locale;

public enum LuceneFileType
{
    SI(0, true),
    CFE(1, true),
    SEGMENTS(2, true),
    CFS(3, false),
    UNKNOWN(-1, false);

    private final int nativeId;
    private final boolean smallFile;

    LuceneFileType(int nativeId, boolean isSmallFile)
    {
        this.nativeId = nativeId;
        this.smallFile = isSmallFile;
    }

    public static LuceneFileType getType(String fileName)
    {
        for (LuceneFileType value : values()) {
            if (fileName.endsWith("." + value.name().toLowerCase(Locale.ENGLISH))) {
                return value;
            }
        }
        if (fileName.contains("segments")) {
            return SEGMENTS;
        }
        return UNKNOWN;
    }

    public static String getFixedFileName(LuceneFileType type)
    {
        return switch (type) {
            case SI -> "v1.si";
            case SEGMENTS -> "segments";
            case CFE -> "v1.cfe";
            case CFS -> "v1.cfs";
            default -> "unknown";
        };
    }

    public int getNativeId()
    {
        return nativeId;
    }

    public boolean isSmallFile()
    {
        return smallFile;
    }
}
