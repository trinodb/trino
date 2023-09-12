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

package io.trino.sql.planner;

import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.spi.connector.WriterScalingOptions;
import io.trino.sql.planner.plan.TableWriterNode;

import java.util.OptionalInt;

public class TestingWriterTarget
        extends TableWriterNode.WriterTarget
{
    @Override
    public String toString()
    {
        return "testing handle";
    }

    @Override
    public boolean supportsMultipleWritersPerPartition(Metadata metadata, Session session)
    {
        return false;
    }

    @Override
    public OptionalInt getMaxWriterTasks(Metadata metadata, Session session)
    {
        return OptionalInt.empty();
    }

    @Override
    public WriterScalingOptions getWriterScalingOptions(Metadata metadata, Session session)
    {
        return WriterScalingOptions.DISABLED;
    }
}
