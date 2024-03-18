
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
public final class VaradaStatsDictionary
        extends VaradaStatsBase
{
    /* This class file is auto-generated from dictionary xml file for statistics and counters */
    private final String group;

    private final LongAdder dictionary_max_exception_count = new LongAdder();
    private final LongAdder dictionary_rejected_elements_count = new LongAdder();
    private final LongAdder dictionary_success_elements_count = new LongAdder();
    private final LongAdder write_dictionaries_count = new LongAdder();
    private final LongAdder dictionary_block_saved_bytes = new LongAdder();
    private final LongAdder dictionary_read_elements_count = new LongAdder();
    private final LongAdder dictionary_loaded_elements_count = new LongAdder();
    private final LongAdder dictionaries_weight = new LongAdder();
    private final LongAdder dictionaries_varlen_str_weight = new LongAdder();
    private final LongAdder dictionary_entries = new LongAdder();
    private final LongAdder dictionary_active_size = new LongAdder();
    private final LongAdder dictionary_evicted_entries = new LongAdder();

    @JsonCreator
    public VaradaStatsDictionary(@JsonProperty("group") String group)
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
    public long getdictionary_max_exception_count()
    {
        return dictionary_max_exception_count.longValue();
    }

    public void incdictionary_max_exception_count()
    {
        dictionary_max_exception_count.increment();
    }

    public void adddictionary_max_exception_count(long val)
    {
        dictionary_max_exception_count.add(val);
    }

    public void setdictionary_max_exception_count(long val)
    {
        dictionary_max_exception_count.reset();
        adddictionary_max_exception_count(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_rejected_elements_count()
    {
        return dictionary_rejected_elements_count.longValue();
    }

    public void incdictionary_rejected_elements_count()
    {
        dictionary_rejected_elements_count.increment();
    }

    public void adddictionary_rejected_elements_count(long val)
    {
        dictionary_rejected_elements_count.add(val);
    }

    public void setdictionary_rejected_elements_count(long val)
    {
        dictionary_rejected_elements_count.reset();
        adddictionary_rejected_elements_count(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_success_elements_count()
    {
        return dictionary_success_elements_count.longValue();
    }

    public void incdictionary_success_elements_count()
    {
        dictionary_success_elements_count.increment();
    }

    public void adddictionary_success_elements_count(long val)
    {
        dictionary_success_elements_count.add(val);
    }

    public void setdictionary_success_elements_count(long val)
    {
        dictionary_success_elements_count.reset();
        adddictionary_success_elements_count(val);
    }

    @JsonIgnore
    @Managed
    public long getwrite_dictionaries_count()
    {
        return write_dictionaries_count.longValue();
    }

    public void incwrite_dictionaries_count()
    {
        write_dictionaries_count.increment();
    }

    public void addwrite_dictionaries_count(long val)
    {
        write_dictionaries_count.add(val);
    }

    public void setwrite_dictionaries_count(long val)
    {
        write_dictionaries_count.reset();
        addwrite_dictionaries_count(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_block_saved_bytes()
    {
        return dictionary_block_saved_bytes.longValue();
    }

    public void incdictionary_block_saved_bytes()
    {
        dictionary_block_saved_bytes.increment();
    }

    public void adddictionary_block_saved_bytes(long val)
    {
        dictionary_block_saved_bytes.add(val);
    }

    public void setdictionary_block_saved_bytes(long val)
    {
        dictionary_block_saved_bytes.reset();
        adddictionary_block_saved_bytes(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_read_elements_count()
    {
        return dictionary_read_elements_count.longValue();
    }

    public void incdictionary_read_elements_count()
    {
        dictionary_read_elements_count.increment();
    }

    public void adddictionary_read_elements_count(long val)
    {
        dictionary_read_elements_count.add(val);
    }

    public void setdictionary_read_elements_count(long val)
    {
        dictionary_read_elements_count.reset();
        adddictionary_read_elements_count(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_loaded_elements_count()
    {
        return dictionary_loaded_elements_count.longValue();
    }

    public void incdictionary_loaded_elements_count()
    {
        dictionary_loaded_elements_count.increment();
    }

    public void adddictionary_loaded_elements_count(long val)
    {
        dictionary_loaded_elements_count.add(val);
    }

    public void setdictionary_loaded_elements_count(long val)
    {
        dictionary_loaded_elements_count.reset();
        adddictionary_loaded_elements_count(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionaries_weight()
    {
        return dictionaries_weight.longValue();
    }

    public void incdictionaries_weight()
    {
        dictionaries_weight.increment();
    }

    public void adddictionaries_weight(long val)
    {
        dictionaries_weight.add(val);
    }

    public void setdictionaries_weight(long val)
    {
        dictionaries_weight.reset();
        adddictionaries_weight(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionaries_varlen_str_weight()
    {
        return dictionaries_varlen_str_weight.longValue();
    }

    public void incdictionaries_varlen_str_weight()
    {
        dictionaries_varlen_str_weight.increment();
    }

    public void adddictionaries_varlen_str_weight(long val)
    {
        dictionaries_varlen_str_weight.add(val);
    }

    public void setdictionaries_varlen_str_weight(long val)
    {
        dictionaries_varlen_str_weight.reset();
        adddictionaries_varlen_str_weight(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_entries()
    {
        return dictionary_entries.longValue();
    }

    public void incdictionary_entries()
    {
        dictionary_entries.increment();
    }

    public void adddictionary_entries(long val)
    {
        dictionary_entries.add(val);
    }

    public void setdictionary_entries(long val)
    {
        dictionary_entries.reset();
        adddictionary_entries(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_active_size()
    {
        return dictionary_active_size.longValue();
    }

    public void incdictionary_active_size()
    {
        dictionary_active_size.increment();
    }

    public void adddictionary_active_size(long val)
    {
        dictionary_active_size.add(val);
    }

    public void setdictionary_active_size(long val)
    {
        dictionary_active_size.reset();
        adddictionary_active_size(val);
    }

    @JsonIgnore
    @Managed
    public long getdictionary_evicted_entries()
    {
        return dictionary_evicted_entries.longValue();
    }

    public void incdictionary_evicted_entries()
    {
        dictionary_evicted_entries.increment();
    }

    public void adddictionary_evicted_entries(long val)
    {
        dictionary_evicted_entries.add(val);
    }

    public void setdictionary_evicted_entries(long val)
    {
        dictionary_evicted_entries.reset();
        adddictionary_evicted_entries(val);
    }

    public static VaradaStatsDictionary create(String group)
    {
        return new VaradaStatsDictionary(group);
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
        ret.put("dictionary_max_exception_count", dictionary_max_exception_count);
        ret.put("dictionary_rejected_elements_count", dictionary_rejected_elements_count);
        ret.put("dictionary_success_elements_count", dictionary_success_elements_count);
        ret.put("write_dictionaries_count", write_dictionaries_count);
        ret.put("dictionary_block_saved_bytes", dictionary_block_saved_bytes);
        ret.put("dictionary_read_elements_count", dictionary_read_elements_count);
        ret.put("dictionary_loaded_elements_count", dictionary_loaded_elements_count);
        ret.put("dictionaries_weight", dictionaries_weight);
        ret.put("dictionaries_varlen_str_weight", dictionaries_varlen_str_weight);
        ret.put("dictionary_entries", dictionary_entries);
        ret.put("dictionary_active_size", dictionary_active_size);
        ret.put("dictionary_evicted_entries", dictionary_evicted_entries);

        return ret;
    }

    @Override
    public void mergeStats(VaradaStatsBase varadaStatsBase)
    {
        if (varadaStatsBase == null) {
            return;
        }
        VaradaStatsDictionary other = (VaradaStatsDictionary) varadaStatsBase;
        this.dictionary_max_exception_count.add(other.dictionary_max_exception_count.longValue());
        this.dictionary_rejected_elements_count.add(other.dictionary_rejected_elements_count.longValue());
        this.dictionary_success_elements_count.add(other.dictionary_success_elements_count.longValue());
        this.write_dictionaries_count.add(other.write_dictionaries_count.longValue());
        this.dictionary_block_saved_bytes.add(other.dictionary_block_saved_bytes.longValue());
        this.dictionary_read_elements_count.add(other.dictionary_read_elements_count.longValue());
        this.dictionary_loaded_elements_count.add(other.dictionary_loaded_elements_count.longValue());
        this.dictionaries_weight.add(other.dictionaries_weight.longValue());
        this.dictionaries_varlen_str_weight.add(other.dictionaries_varlen_str_weight.longValue());
        this.dictionary_entries.add(other.dictionary_entries.longValue());
        this.dictionary_active_size.add(other.dictionary_active_size.longValue());
        this.dictionary_evicted_entries.add(other.dictionary_evicted_entries.longValue());
    }

    @Override
    public void reset()
    {
        dictionary_max_exception_count.reset();
        dictionary_rejected_elements_count.reset();
        dictionary_success_elements_count.reset();
        write_dictionaries_count.reset();
        dictionary_block_saved_bytes.reset();
        dictionary_read_elements_count.reset();
        dictionary_loaded_elements_count.reset();
        dictionaries_weight.reset();
        dictionaries_varlen_str_weight.reset();
        dictionary_entries.reset();
        dictionary_active_size.reset();
        dictionary_evicted_entries.reset();
    }

    @Override
    public Map<String, Object> statsCounterMapper()
    {
        Map<String, Object> res = new HashMap<>();
        res.put(getJmxKey() + ":dictionary_read_elements_count", dictionary_read_elements_count.longValue());
        res.put(getJmxKey() + ":dictionary_loaded_elements_count", dictionary_loaded_elements_count.longValue());
        res.put(getJmxKey() + ":dictionary_active_size", dictionary_active_size.longValue());
        res.put(getJmxKey() + ":dictionary_evicted_entries", dictionary_evicted_entries.longValue());
        return res;
    }

    @Override
    protected Map<String, Object> deltaPrintFields()
    {
        Map<String, Object> res = new HashMap<>();
        res.put("dictionary_max_exception_count", getdictionary_max_exception_count());
        res.put("dictionary_rejected_elements_count", getdictionary_rejected_elements_count());
        res.put("dictionary_success_elements_count", getdictionary_success_elements_count());
        res.put("write_dictionaries_count", getwrite_dictionaries_count());
        res.put("dictionary_block_saved_bytes", getdictionary_block_saved_bytes());
        res.put("dictionary_read_elements_count", getdictionary_read_elements_count());
        res.put("dictionary_loaded_elements_count", getdictionary_loaded_elements_count());
        res.put("dictionaries_weight", getdictionaries_weight());
        res.put("dictionaries_varlen_str_weight", getdictionaries_varlen_str_weight());
        res.put("dictionary_entries", getdictionary_entries());
        res.put("dictionary_active_size", getdictionary_active_size());
        res.put("dictionary_evicted_entries", getdictionary_evicted_entries());
        return res;
    }

    @Override
    protected Map<String, Object> statePrintFields()
    {
        return new HashMap<>();
    }

    public int getNumberOfMetrics()
    {
        return 12;
    }
}
