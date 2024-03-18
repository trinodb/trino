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
package io.trino.plugin.varada.dictionary;

public class DictionaryToWrite
{
    private final Object[] dictionary;
    private final int size;
    private final int recTypeLength;
    private final int weight;

    DictionaryToWrite(Object[] dictionary,
            int size,
            int recTypeLength,
            int weight)
    {
        this.dictionary = dictionary;
        this.size = size;
        this.recTypeLength = recTypeLength;
        this.weight = weight;
    }

    public Object get(int index)
    {
        // we have no protection on index < size since all usages are in a for loop from 0 to getSize()
        return dictionary[index];
    }

    public int getRecTypeLength()
    {
        return recTypeLength;
    }

    public int getSize()
    {
        return size;
    }

    public int getWeight()
    {
        return weight;
    }

    @Override
    public String toString()
    {
        return "DictionaryToWrite{" +
                "size=" + size +
                ", recTypeLength=" + recTypeLength +
                ", weight=" + weight +
                '}';
    }
}
