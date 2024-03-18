
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
public final class VaradaStatsBufferAllocator
        extends VaradaStatsBase
{
    /* This class file is auto-generated from bufferAllocator xml file for statistics and counters */
    private final String group;

    private final LongAdder available_load_bundles = new LongAdder();
    private final LongAdder reader_taken = new LongAdder();
    private final LongAdder writer_taken = new LongAdder();
    private final LongAdder allowed_loaders = new LongAdder();
    private final LongAdder partial_handled_columns = new LongAdder();
    private final LongAdder predicate_buffer_small_alloc = new LongAdder();
    private final LongAdder predicate_buffer_medium_alloc = new LongAdder();
    private final LongAdder predicate_buffer_large_alloc = new LongAdder();

    @JsonCreator
    public VaradaStatsBufferAllocator(@JsonProperty("group") String group)
    {
        super(createKey(group), VaradaStatType.Worker);

        this.group = group;
    }

    @JsonProperty
    @Managed
    public String getGroup()
    {
        return group;
    }

    @JsonIgnore
    @Managed
    public long getavailable_load_bundles()
    {
        return available_load_bundles.longValue();
    }

    public void incavailable_load_bundles()
    {
        available_load_bundles.increment();
    }

    public void addavailable_load_bundles(long val)
    {
        available_load_bundles.add(val);
    }

    public void setavailable_load_bundles(long val)
    {
        available_load_bundles.reset();
        addavailable_load_bundles(val);
    }

    @JsonIgnore
    @Managed
    public long getreader_taken()
    {
        return reader_taken.longValue();
    }

    public void increader_taken()
    {
        reader_taken.increment();
    }

    public void addreader_taken(long val)
    {
        reader_taken.add(val);
    }

    public void setreader_taken(long val)
    {
        reader_taken.reset();
        addreader_taken(val);
    }

    @JsonIgnore
    @Managed
    public long getwriter_taken()
    {
        return writer_taken.longValue();
    }

    public void incwriter_taken()
    {
        writer_taken.increment();
    }

    public void addwriter_taken(long val)
    {
        writer_taken.add(val);
    }

    public void setwriter_taken(long val)
    {
        writer_taken.reset();
        addwriter_taken(val);
    }

    @JsonIgnore
    @Managed
    public long getallowed_loaders()
    {
        return allowed_loaders.longValue();
    }

    public void incallowed_loaders()
    {
        allowed_loaders.increment();
    }

    public void addallowed_loaders(long val)
    {
        allowed_loaders.add(val);
    }

    public void setallowed_loaders(long val)
    {
        allowed_loaders.reset();
        addallowed_loaders(val);
    }

    @JsonIgnore
    @Managed
    public long getpartial_handled_columns()
    {
        return partial_handled_columns.longValue();
    }

    public void incpartial_handled_columns()
    {
        partial_handled_columns.increment();
    }

    public void addpartial_handled_columns(long val)
    {
        partial_handled_columns.add(val);
    }

    public void setpartial_handled_columns(long val)
    {
        partial_handled_columns.reset();
        addpartial_handled_columns(val);
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_small_alloc()
    {
        return predicate_buffer_small_alloc.longValue();
    }

    public void incpredicate_buffer_small_alloc()
    {
        predicate_buffer_small_alloc.increment();
    }

    public void addpredicate_buffer_small_alloc(long val)
    {
        predicate_buffer_small_alloc.add(val);
    }

    public void setpredicate_buffer_small_alloc(long val)
    {
        predicate_buffer_small_alloc.reset();
        addpredicate_buffer_small_alloc(val);
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_medium_alloc()
    {
        return predicate_buffer_medium_alloc.longValue();
    }

    public void incpredicate_buffer_medium_alloc()
    {
        predicate_buffer_medium_alloc.increment();
    }

    public void addpredicate_buffer_medium_alloc(long val)
    {
        predicate_buffer_medium_alloc.add(val);
    }

    public void setpredicate_buffer_medium_alloc(long val)
    {
        predicate_buffer_medium_alloc.reset();
        addpredicate_buffer_medium_alloc(val);
    }

    @JsonIgnore
    @Managed
    public long getpredicate_buffer_large_alloc()
    {
        return predicate_buffer_large_alloc.longValue();
    }

    public void incpredicate_buffer_large_alloc()
    {
        predicate_buffer_large_alloc.increment();
    }

    public void addpredicate_buffer_large_alloc(long val)
    {
        predicate_buffer_large_alloc.add(val);
    }

    public void setpredicate_buffer_large_alloc(long val)
    {
        predicate_buffer_large_alloc.reset();
        addpredicate_buffer_large_alloc(val);
    }

    public static VaradaStatsBufferAllocator create(String group)
    {
        return new VaradaStatsBufferAllocator(group);
    }

    public static String createKey(String group)
    {
        return new StringJoiner(".").add(group).toString();
    }

    @Override
    public void merge(VaradaStatsBase varadaStatsBase)
    {
    }

    @Override
    public Map<String, LongAdder> getCounters()
    {
        Map<String, LongAdder> ret = new HashMap<>();
        ret.put("available_load_bundles", available_load_bundles);
        ret.put("reader_taken", reader_taken);
        ret.put("writer_taken", writer_taken);
        ret.put("allowed_loaders", allowed_loaders);
        ret.put("partial_handled_columns", partial_handled_columns);
        ret.put("predicate_buffer_small_alloc", predicate_buffer_small_alloc);
        ret.put("predicate_buffer_medium_alloc", predicate_buffer_medium_alloc);
        ret.put("predicate_buffer_large_alloc", predicate_buffer_large_alloc);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsBufferAllocator other = (VaradaStatsBufferAllocator) varadaStatsBase;
        this.available_load_bundles.add(other.available_load_bundles.longValue());
        this.reader_taken.add(other.reader_taken.longValue());
        this.writer_taken.add(other.writer_taken.longValue());
        this.allowed_loaders.add(other.allowed_loaders.longValue());
        this.partial_handled_columns.add(other.partial_handled_columns.longValue());
        this.predicate_buffer_small_alloc.add(other.predicate_buffer_small_alloc.longValue());
        this.predicate_buffer_medium_alloc.add(other.predicate_buffer_medium_alloc.longValue());
        this.predicate_buffer_large_alloc.add(other.predicate_buffer_large_alloc.longValue());
    }

    @Override
    public void reset()
    {
        available_load_bundles.reset();
        reader_taken.reset();
        writer_taken.reset();
        allowed_loaders.reset();
        partial_handled_columns.reset();
        predicate_buffer_small_alloc.reset();
        predicate_buffer_medium_alloc.reset();
        predicate_buffer_large_alloc.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":available_load_bundles", available_load_bundles.longValue());
        res.put(getJmxKey() + ":reader_taken", reader_taken.longValue());
        res.put(getJmxKey() + ":writer_taken", writer_taken.longValue());
        res.put(getJmxKey() + ":allowed_loaders", allowed_loaders.longValue());
        res.put(getJmxKey() + ":partial_handled_columns", partial_handled_columns.longValue());
        res.put(getJmxKey() + ":predicate_buffer_small_alloc", predicate_buffer_small_alloc.longValue());
        res.put(getJmxKey() + ":predicate_buffer_medium_alloc", predicate_buffer_medium_alloc.longValue());
        res.put(getJmxKey() + ":predicate_buffer_large_alloc", predicate_buffer_large_alloc.longValue());
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
        return 8;
    }
}
