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
package io.trino.plugin.hudi.files;

import static java.util.Objects.requireNonNull;

public record HudiFileGroupId(String partitionPath, String fileId)
        implements Comparable<HudiFileGroupId>
{
    public HudiFileGroupId
    {
        requireNonNull(partitionPath, "partitionPath is null");
        requireNonNull(fileId, "partitionPath is null");
    }

    @Override
    public String toString()
    {
        return "HoodieFileGroupId{partitionPath='" + partitionPath + '\'' + ", fileId='" + fileId + '\'' + '}';
    }

    @Override
    public int compareTo(HudiFileGroupId o)
    {
        int ret = partitionPath.compareTo(o.partitionPath);
        if (ret == 0) {
            ret = fileId.compareTo(o.fileId);
        }
        return ret;
    }
}
