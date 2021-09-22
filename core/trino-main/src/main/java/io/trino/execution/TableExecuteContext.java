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
package io.trino.execution;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class TableExecuteContext
{
    private final AtomicReference<List<Object>> splitsInfo = new AtomicReference<>();

    public void setSplitsInfo(List<Object> splitsInfo)
    {
        boolean success = this.splitsInfo.compareAndSet(null, splitsInfo);
        if (!success) {
            throw new IllegalStateException("splitsInfo already set to " + this.splitsInfo.get());
        }
    }

    public List<Object> getSplitsInfo()
    {
        List<Object> splitsInfo = this.splitsInfo.get();
        if (splitsInfo == null) {
            throw new IllegalStateException("splits info not set yet");
        }
        return splitsInfo;
    }
}
