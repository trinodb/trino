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
package io.trino.plugin.hive.util;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;

import java.io.IOException;

public class CustomSplit
        extends FileSplit implements InputSplit
{
    private InputSplit embeddedSplit;
    private int customField;

    protected CustomSplit()
    {
    }

    public CustomSplit(InputSplit embeddedSplit, int customField)
    {
        this.embeddedSplit = embeddedSplit;
        this.customField = customField;
    }

    public long getLength()
    {
        try {
            return this.embeddedSplit.getLength();
        }
        catch (IOException var2) {
            throw new RuntimeException(var2);
        }
    }

    public String[] getLocations() throws IOException
    {
        return this.embeddedSplit.getLocations();
    }

    public Path getPath()
    {
        if (this.embeddedSplit instanceof FileSplit) {
            return ((FileSplit) this.embeddedSplit).getPath();
        }
        else {
            throw new RuntimeException(this.embeddedSplit + " is not a FileSplit");
        }
    }

    public long getStart()
    {
        return this.embeddedSplit instanceof FileSplit ? ((FileSplit) this.embeddedSplit).getStart() : 0L;
    }

    public InputSplit getEmbeddedSplit()
    {
        return this.embeddedSplit;
    }

    public long getCustomField()
    {
        return this.customField;
    }

    public String toString()
    {
        return "CustomSplit{embeddedSplit=" + this.embeddedSplit + ", customField=" + this.customField + '}';
    }
}
