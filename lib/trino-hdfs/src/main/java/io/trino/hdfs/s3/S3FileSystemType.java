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
package io.trino.hdfs.s3;

public enum S3FileSystemType
{
    TRINO,
    /**
     * @deprecated EMRFS was used to workaround S3's lack of strong consistency and is no longer needed
     * per <a href="https://aws.amazon.com/blogs/aws/amazon-s3-update-strong-read-after-write-consistency/">AWS's announcement</a>.
     */
    @Deprecated
    EMRFS,
    HADOOP_DEFAULT,
}
