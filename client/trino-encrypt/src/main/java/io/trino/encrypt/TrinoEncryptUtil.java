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
package io.trino.encrypt;

public final class TrinoEncryptUtil
{
    private TrinoEncryptUtil() {}

    public static void main(String[] args)
    {
        if (2 > args.length) {
            System.out.println("***********************INTRODUCTION********************** \n"
                    + "[NOTICE] Please execute with the two sensitive parameters \n"
                    + "param 1: the key for encrytion \n"
                    + "param 2: the string to be encrypted \n");
            return;
        }

        StringBuilder builder = new StringBuilder();
        builder.append("***********************ENCRYPT KEY***********************\n");
        builder.append("%s\n\n");
        builder.append("**************************INPUT**************************\n");
        builder.append("%s\n\n");
        builder.append("**************************OUTPUT*************************\n");
        builder.append("%s\n");
        TrinoEncryptor encryptor = new TrinoEncryptor();
        System.out.println(String.format(builder.toString(), args[0], args[1], encryptor.encrypt(args[0], args[1])));
    }
}
