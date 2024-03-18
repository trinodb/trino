
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
package io.trino.plugin.warp.gen.stats;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.plugin.varada.metrics.VaradaStatType;
import io.trino.plugin.varada.metrics.VaradaStatsBase;
import org.weakref.jmx.Managed;

import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.LongAdder;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.ANY, getterVisibility = JsonAutoDetect.Visibility.ANY, setterVisibility = JsonAutoDetect.Visibility.ANY)
@SuppressWarnings({"checkstyle:MemberName", "checkstyle:ParameterName"})
public final class VaradaStatsLuceneIndexer
        extends VaradaStatsBase
{
    /* This class file is auto-generated from luceneIndexer xml file for statistics and counters */
    private final String group;
    private final String weIx;

    private final LongAdder failedAddDoc = new LongAdder();
    private final LongAdder failedReset = new LongAdder();
    private final LongAdder local_fs_Count = new LongAdder();
    private final LongAdder local_fs = new LongAdder();
    private final LongAdder merge_Count = new LongAdder();
    private final LongAdder merge = new LongAdder();
    private final LongAdder addDoc_Count = new LongAdder();
    private final LongAdder addDoc = new LongAdder();
    private final LongAdder copy_Count = new LongAdder();
    private final LongAdder copy = new LongAdder();

    @JsonCreator
    public VaradaStatsLuceneIndexer(@JsonProperty("group") String group, @JsonProperty("weIx") String weIx)
    {
        super(createKey(group, weIx), VaradaStatType.Worker);

        this.group = group;
        this.weIx = weIx;
    }

    @JsonProperty
    @Managed
    public String getGroup()
    {
        return group;
    }

    @JsonProperty
    @Managed
    public String getWeIx()
    {
        return weIx;
    }

    @JsonIgnore
    @Managed
    public long getfailedAddDoc()
    {
        return failedAddDoc.longValue();
    }

    public void incfailedAddDoc()
    {
        failedAddDoc.increment();
    }

    public void addfailedAddDoc(long val)
    {
        failedAddDoc.add(val);
    }

    public void setfailedAddDoc(long val)
    {
        failedAddDoc.reset();
        addfailedAddDoc(val);
    }

    @JsonIgnore
    @Managed
    public long getfailedReset()
    {
        return failedReset.longValue();
    }

    public void incfailedReset()
    {
        failedReset.increment();
    }

    public void addfailedReset(long val)
    {
        failedReset.add(val);
    }

    public void setfailedReset(long val)
    {
        failedReset.reset();
        addfailedReset(val);
    }

    @JsonIgnore
    @Managed
    public long getlocal_fs_Count()
    {
        return local_fs_Count.longValue();
    }

    @Managed
    public long getlocal_fs_Average()
    {
        if (local_fs_Count.longValue() == 0) {
            return 0;
        }
        return local_fs.longValue() / local_fs_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getlocal_fs()
    {
        return local_fs.longValue();
    }

    public void addlocal_fs(long val)
    {
        local_fs.add(val);
        local_fs_Count.add(1);
    }

    @JsonIgnore
    @Managed
    public long getmerge_Count()
    {
        return merge_Count.longValue();
    }

    @Managed
    public long getmerge_Average()
    {
        if (merge_Count.longValue() == 0) {
            return 0;
        }
        return merge.longValue() / merge_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getmerge()
    {
        return merge.longValue();
    }

    public void addmerge(long val)
    {
        merge.add(val);
        merge_Count.add(1);
    }

    @JsonIgnore
    @Managed
    public long getaddDoc_Count()
    {
        return addDoc_Count.longValue();
    }

    @Managed
    public long getaddDoc_Average()
    {
        if (addDoc_Count.longValue() == 0) {
            return 0;
        }
        return addDoc.longValue() / addDoc_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getaddDoc()
    {
        return addDoc.longValue();
    }

    public void addaddDoc(long val)
    {
        addDoc.add(val);
        addDoc_Count.add(1);
    }

    @JsonIgnore
    @Managed
    public long getcopy_Count()
    {
        return copy_Count.longValue();
    }

    @Managed
    public long getcopy_Average()
    {
        if (copy_Count.longValue() == 0) {
            return 0;
        }
        return copy.longValue() / copy_Count.longValue();
    }

    @JsonIgnore
    @Managed
    public long getcopy()
    {
        return copy.longValue();
    }

    public void addcopy(long val)
    {
        copy.add(val);
        copy_Count.add(1);
    }

    public static VaradaStatsLuceneIndexer create(String group, String weIx)
    {
        return new VaradaStatsLuceneIndexer(group, weIx);
    }

    public static String createKey(String group, String weIx)
    {
        return new StringJoiner(".").add(group).add(weIx).toString();
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("failedAddDoc", failedAddDoc);
        ret.put("failedReset", failedReset);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsLuceneIndexer other = (VaradaStatsLuceneIndexer) varadaStatsBase;
        this.failedAddDoc.add(other.failedAddDoc.longValue());
        this.failedReset.add(other.failedReset.longValue());
        this.local_fs.add(other.local_fs.longValue());
        this.local_fs_Count.add(other.local_fs_Count.longValue());
        this.merge.add(other.merge.longValue());
        this.merge_Count.add(other.merge_Count.longValue());
        this.addDoc.add(other.addDoc.longValue());
        this.addDoc_Count.add(other.addDoc_Count.longValue());
        this.copy.add(other.copy.longValue());
        this.copy_Count.add(other.copy_Count.longValue());
    }

    @Override
    public void reset()
    {
        failedAddDoc.reset();
        failedReset.reset();
        local_fs.reset();
        local_fs_Count.reset();
        merge.reset();
        merge_Count.reset();
        addDoc.reset();
        addDoc_Count.reset();
        copy.reset();
        copy_Count.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":failedAddDoc", failedAddDoc.longValue());
        res.put(getJmxKey() + ":failedReset", failedReset.longValue());
        res.put(getJmxKey() + ":local_fs", local_fs.longValue());
        res.put(getJmxKey() + ":local_fs_Count", local_fs_Count.longValue());
        res.put(getJmxKey() + ":merge", merge.longValue());
        res.put(getJmxKey() + ":merge_Count", merge_Count.longValue());
        res.put(getJmxKey() + ":addDoc", addDoc.longValue());
        res.put(getJmxKey() + ":addDoc_Count", addDoc_Count.longValue());
        res.put(getJmxKey() + ":copy", copy.longValue());
        res.put(getJmxKey() + ":copy_Count", copy_Count.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        return new HashMap<>();
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 2;
    }
}
