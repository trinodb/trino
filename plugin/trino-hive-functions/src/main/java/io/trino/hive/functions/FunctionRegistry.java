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

package io.trino.hive.functions;

import io.airlift.log.Logger;
import io.trino.spi.function.Signature;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.ql.udf.xml.GenericUDFXPath;

import java.util.*;
import java.util.stream.Collectors;

public final class FunctionRegistry {

    static private Logger LOG = Logger.get(FunctionRegistry.class);

    private FunctionRegistry() {
    }

    // registry for hive functions
    private static final Registry system = new Registry(true);
    private static List<FunctionInfo> functions = List.of();

    static {
        // register genericUDF
        Map<String, Class<? extends GenericUDF>> nameToClass = new HashMap<String, Class<? extends GenericUDF>>() {
            {
                put("concat", GenericUDFConcat.class);
                put("substring_index", GenericUDFSubstringIndex.class);
                put("lpad", GenericUDFLpad.class);
                put("rpad", GenericUDFRpad.class);
                put("levenshtein", GenericUDFLevenshtein.class);
                put("soundex", GenericUDFSoundex.class);
                put("size", GenericUDFSize.class);
                put("round", GenericUDFRound.class);
                put("bround", GenericUDFBRound.class);
                put("floor", GenericUDFFloor.class);
                put("cbrt", GenericUDFCbrt.class);
                put("ceil", GenericUDFCeil.class);
                put("ceiling", GenericUDFCeil.class);
                put("abs", GenericUDFAbs.class);
                put("sq_count_check", GenericUDFSQCountCheck.class);
                put("enforce_constraint", GenericUDFEnforceConstraint.class);
                put("pmod", GenericUDFPosMod.class);
                put("power", GenericUDFPower.class);
                put("pow", GenericUDFPower.class);
                put("factorial", GenericUDFFactorial.class);
                put("sha2", GenericUDFSha2.class);
                put("aes_encrypt", GenericUDFAesEncrypt.class);
                put("aes_decrypt", GenericUDFAesDecrypt.class);
                put("encode", GenericUDFEncode.class);
                put("decode", GenericUDFDecode.class);
                put("upper", GenericUDFUpper.class);
                put("lower", GenericUDFLower.class);
                put("ucase", GenericUDFUpper.class);
                put("lcase", GenericUDFLower.class);
                put("trim", GenericUDFTrim.class);
                put("ltrim", GenericUDFLTrim.class);
                put("rtrim", GenericUDFRTrim.class);
                put("length", GenericUDFLength.class);
                put("character_length", GenericUDFCharacterLength.class);
                put("char_length", GenericUDFCharacterLength.class);
                put("octet_length", GenericUDFOctetLength.class);
                put("field", GenericUDFField.class);
                put("initcap", GenericUDFInitCap.class);
                put("likeany", GenericUDFLikeAny.class);
                put("likeall", GenericUDFLikeAll.class);
                put("rlike", GenericUDFRegExp.class);
                put("regexp", GenericUDFRegExp.class);
                put("nvl", GenericUDFNvl.class);
                put("split", GenericUDFSplit.class);
                put("str_to_map", GenericUDFStringToMap.class);
                put("translate", GenericUDFTranslate.class);
                put("positive", GenericUDFOPPositive.class);
                put("negative", GenericUDFOPNegative.class);
                put("quarter", GenericUDFQuarter.class);
                put("to_date", GenericUDFDate.class);
                put("last_day", GenericUDFLastDay.class);
                put("next_day", GenericUDFNextDay.class);
                put("trunc", GenericUDFTrunc.class);
                put("date_format", GenericUDFDateFormat.class);
                put("date_add", GenericUDFDateAdd.class);
                put("date_sub", GenericUDFDateSub.class);
                put("datediff", GenericUDFDateDiff.class);
                put("add_months", GenericUDFAddMonths.class);
                put("months_between", GenericUDFMonthsBetween.class);
                put("xpath", GenericUDFXPath.class);
                put("grouping", GenericUDFGrouping.class);
                put("current_database", UDFCurrentDB.class);
                put("current_date", GenericUDFCurrentDate.class);
                put("current_timestamp", GenericUDFCurrentTimestamp.class);
                put("current_user", GenericUDFCurrentUser.class);
                put("current_groups", GenericUDFCurrentGroups.class);
                put("logged_in_user", GenericUDFLoggedInUser.class);
                put("restrict_information_schema", GenericUDFRestrictInformationSchema.class);
                put("isnull", GenericUDFOPNull.class);
                put("isnotnull", GenericUDFOPNotNull.class);
                put("istrue", GenericUDFOPTrue.class);
                put("isnottrue", GenericUDFOPNotTrue.class);
                put("isfalse", GenericUDFOPFalse.class);
                put("isnotfalse", GenericUDFOPNotFalse.class);
                put("between", GenericUDFBetween.class);
                put("in_bloom_filter", GenericUDFInBloomFilter.class);
                put("date", GenericUDFToDate.class);
                put("timestamp", GenericUDFTimestamp.class);
                put("timestamp with local time zone", GenericUDFToTimestampLocalTZ.class);
                put("interval_year_month", GenericUDFToIntervalYearMonth.class);
                put("interval_day_time", GenericUDFToIntervalDayTime.class);
                put("binary", GenericUDFToBinary.class);
                put("decimal", GenericUDFToDecimal.class);
                put("varchar", GenericUDFToVarchar.class);
                put("char", GenericUDFToChar.class);
                put("reflect", GenericUDFReflect.class);
                put("reflect2", GenericUDFReflect2.class);
                put("java_method", GenericUDFReflect.class);
                put("array", GenericUDFArray.class);
                put("assert_true", GenericUDFAssertTrue.class);
                put("assert_true_oom", GenericUDFAssertTrueOOM.class);
                put("map", GenericUDFMap.class);
                put("struct", GenericUDFStruct.class);
                put("named_struct", GenericUDFNamedStruct.class);
                put("create_union", GenericUDFUnion.class);
                put("extract_union", GenericUDFExtractUnion.class);
                put("case", GenericUDFCase.class);
                put("when", GenericUDFWhen.class);
                put("nullif", GenericUDFNullif.class);
                put("hash", GenericUDFHash.class);
                put("murmur_hash", GenericUDFMurmurHash.class);
                put("coalesce", GenericUDFCoalesce.class);
                put("index", GenericUDFIndex.class);
                put("in_file", GenericUDFInFile.class);
                put("instr", GenericUDFInstr.class);
                put("locate", GenericUDFLocate.class);
                put("elt", GenericUDFElt.class);
                put("concat_ws", GenericUDFConcatWS.class);
                put("sort_array", GenericUDFSortArray.class);
                put("sort_array_by", GenericUDFSortArrayByField.class);
                put("array_contains", GenericUDFArrayContains.class);
                put("sentences", GenericUDFSentences.class);
                put("map_keys", GenericUDFMapKeys.class);
                put("map_values", GenericUDFMapValues.class);
                put("format_number", GenericUDFFormatNumber.class);
                put("printf", GenericUDFPrintf.class);
                put("greatest", GenericUDFGreatest.class);
                put("least", GenericUDFLeast.class);
                put("cardinality_violation", GenericUDFCardinalityViolation.class);
                put("width_bucket", GenericUDFWidthBucket.class);
                put("from_utc_timestamp", GenericUDFFromUtcTimestamp.class);
                put("to_utc_timestamp", GenericUDFToUtcTimestamp.class);
                put("unix_timestamp", GenericUDFUnixTimeStamp.class);
                put("to_unix_timestamp", GenericUDFToUnixTimeStamp.class);
                put("internal_interval", GenericUDFInternalInterval.class);
                put("to_epoch_milli", GenericUDFEpochMilli.class);
                put("lead", GenericUDFLead.class);
                put("lag", GenericUDFLag.class);
            }
        };

        functions = nameToClass.entrySet().stream()
                .map(entry -> {
                    try {
                        return system.registerGenericUDF(entry.getKey(), entry.getValue());
                    } catch (Exception e) {
                        e.printStackTrace();
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());

    }

    public static FunctionInfo getFunctionInfo(String functionName) throws SemanticException {
        return system.getFunctionInfo(functionName);
    }

    public static Set<String> getCurrentFunctionNames() {
        return system.getCurrentFunctionNames();
    }

    public static Optional<Signature> getSignature(String functionName) {
        if (functions.contains(functionName)) {
            try {
                Class<?> functionClass = getFunctionInfo(functionName).getFunctionClass();
                return Optional.of(
                        Signature.builder()
                                .name(functionName)
                                .variableArity().build());
            }
            catch (SemanticException | NullPointerException e) {
                LOG.error("Class of function " + functionName + " not found", e);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }
}
