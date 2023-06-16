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
package io.trino.plugin.openpolicyagent;

public class FunctionalHelpers
{
    @FunctionalInterface
    public static interface Consumer3<T1, T2, T3>
    {
        void accept(T1 t1, T2 t2, T3 t3);
    }

    @FunctionalInterface
    public static interface Consumer4<T1, T2, T3, T4>
    {
        void accept(T1 t1, T2 t2, T3 t3, T4 t4);
    }

    @FunctionalInterface
    public static interface Consumer5<T1, T2, T3, T4, T5>
    {
        void accept(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5);
    }

    @FunctionalInterface
    public static interface Consumer6<T1, T2, T3, T4, T5, T6>
    {
        void accept(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5, T6 t6);
    }

    public static class Pair<T, U>
    {
        private T first;
        private U second;

        public T getFirst()
        {
            return this.first;
        }

        public U getSecond()
        {
            return this.second;
        }

        public Pair(T first, U second)
        {
            this.first = first;
            this.second = second;
        }

        public static <T, U> Pair<T, U> of(T first, U second)
        {
            return new Pair(first, second);
        }
    }
}
