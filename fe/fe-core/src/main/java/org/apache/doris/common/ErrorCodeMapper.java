// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.common;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class ErrorCodeMapper {

    private static final Map<Pattern, ErrorCode> errorMap = new HashMap<>();

    static {
        errorMap.put(Pattern.compile("Can't create database"), ErrorCode.ERR_DB_CREATE_EXISTS);
        errorMap.put(Pattern.compile("No database selected"), ErrorCode.ERR_NO_DB_ERROR);
        errorMap.put(Pattern.compile("Current database is not set"), ErrorCode.ERR_NO_DB_ERROR);
        errorMap.put(Pattern.compile("Unknown database"), ErrorCode.ERR_BAD_DB_ERROR);
        errorMap.put(Pattern.compile("Database \\[.*\\] does not exist"), ErrorCode.ERR_BAD_DB_ERROR);
        errorMap.put(Pattern.compile("Unknown column"), ErrorCode.ERR_BAD_FIELD_ERROR);
        errorMap.put(Pattern.compile("Not unique table/alias"), ErrorCode.ERR_NONUNIQ_TABLE);
        errorMap.put(Pattern.compile("Unknown thread id"), ErrorCode.ERR_NO_SUCH_THREAD);
        errorMap.put(Pattern.compile("Unknown error"), ErrorCode.ERR_UNKNOWN_ERROR);
        errorMap.put(Pattern.compile("org.apache.hadoop.hive.metastore.api.MetaException"),
                ErrorCode.ERR_TABLEACCESS_DENIED_ERROR);
        errorMap.put(Pattern.compile("has no privilege to query in catalog"),
                ErrorCode.ERR_CATALOG_ACCESS_DENIED_ERROR);
        errorMap.put(Pattern.compile("Access denied"), ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR);
        errorMap.put(Pattern.compile("can't be set to the value of"), ErrorCode.ERR_LOCAL_VARIABLE);
        errorMap.put(Pattern.compile("Unknown system variable"), ErrorCode.ERR_LOCAL_VARIABLE);
        errorMap.put(Pattern.compile("Every derived table must have its own alias"),
                ErrorCode.ERR_DERIVED_MUST_HAVE_ALIAS);
        errorMap.put(Pattern.compile("is ambiguous"), ErrorCode.ERR_AMBIGUOUS_FIELD_TERM);
        errorMap.put(Pattern.compile("The parameter 2 or parameter 3 of LAG/LEAD must be a constant value"),
                ErrorCode.ERR_NO_CONST_EXPR_IN_RANGE_OR_LIST_ERROR);
        errorMap.put(Pattern.compile("Can not found function"), ErrorCode.ERR_FUNC_INEXISTENT_NAME_COLLISION);
        errorMap.put(Pattern.compile("show query/load profile syntax is a deprecated feature"),
                ErrorCode.ERR_WARN_DEPRECATED_SYNTAX_NO_REPLACEMENT);
        errorMap.put(Pattern.compile("send fragments failed"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("io.grpc.StatusRuntimeException"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("no viable alternative at input"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("mismatched input"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("java.lang.NullPointerException"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("Cannot invoke"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("Failed to get query fragments context"), ErrorCode.ERR_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("There is no scanNode Backend available"), ErrorCode.ERR_BACKEND_OFFLINE);
        errorMap.put(Pattern.compile("No available backends"), ErrorCode.ERR_BACKEND_OFFLINE);
        errorMap.put(Pattern.compile("Timeout"), ErrorCode.ERR_EXECUTE_TIMEOUT);
        errorMap.put(Pattern.compile("Unknown catalog"), ErrorCode.ERR_UNKNOWN_CATALOG);
        errorMap.put(Pattern.compile("No catalog found with name"), ErrorCode.ERR_UNKNOWN_CATALOG);
        errorMap.put(Pattern.compile("sum requires a numeric or boolean parameter"),
                ErrorCode.ERR_SUM_REQUIRES_NUMERIC_OR_BOOLEAN);
        errorMap.put(Pattern.compile("we meet an error when parsing"), ErrorCode.ERR_SQL_PARSING_ERROR);
        errorMap.put(Pattern.compile("Unsupported hive input format"),
                ErrorCode.ERR_UNSUPPORTED_HIVE_INPUT_FORMAT);
        errorMap.put(Pattern.compile("not in aggregate's output"), ErrorCode.ERR_NOT_IN_AGGREGATE_OUTPUT);
        errorMap.put(Pattern.compile("Nereids cost too much time"), ErrorCode.ERR_NEREIDS_TIMEOUT);
        errorMap.put(Pattern.compile("Invalid call to toSlot on unbound object"),
                ErrorCode.ERR_INVALID_CALL_TO_SLOT);
        errorMap.put(Pattern.compile("get file split failed for table"),
                ErrorCode.ERR_GET_FILE_SPLIT_FAILED_FOR_TABLE);
        errorMap.put(Pattern.compile("failed to open transaction"), ErrorCode.ERR_FAILED_TO_OPEN_TRANSACTION);
        errorMap.put(Pattern.compile("is not mutable"), ErrorCode.ERR_CONFIG_NOT_MUTABLE);
        errorMap.put(Pattern.compile("Catalog had already exist with name"),
                ErrorCode.ERR_CATALOG_ALREADY_EXISTS);
        errorMap.put(Pattern.compile("Can not find the compatibility function signature"),
                ErrorCode.ERR_COMPATIBILITY_FUNCTION_SIGNATURE_NOT_FOUND);
        errorMap.put(Pattern.compile("Backend process epoch changed"),
                ErrorCode.ERR_BACKEND_PROCESS_EPOCH_CHANGED);
        errorMap.put(Pattern.compile("Backend Backend"), ErrorCode.ERR_BACKEND_ERROR);
        errorMap.put(Pattern.compile("should be grouped by"), ErrorCode.ERR_SHOULD_BE_GROUPED_BY);
        errorMap.put(Pattern.compile("Failed to connect to backend"), ErrorCode.ERR_FAILED_TO_CONNECT_BACKEND);
        errorMap.put(Pattern.compile("Failed to get batch of split source"),
                ErrorCode.ERR_FETCH_SPLIT_BATCH_INTERNAL_ERROR);
        errorMap.put(Pattern.compile("failed to init reader"), ErrorCode.ERR_FAILED_TO_INIT_READER);
        errorMap.put(Pattern.compile("MinorGC kill overcommit query"),
                ErrorCode.ERR_MINORGC_KILL_OVERCOMMIT_QUERY);
        errorMap.put(Pattern.compile("cancel top memory used query"),
                ErrorCode.ERR_PROCESS_MEMORY_NOT_ENOUGH_CANCEL_QUERY);
        errorMap.put(Pattern.compile("MEM_ALLOC_FAILED"), ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED);
        errorMap.put(Pattern.compile("MEM_LIMIT_EXCEEDED"), ErrorCode.ERR_PRECATCH_MEM_LIMIT_EXCEEDED);
        errorMap.put(Pattern.compile("failed to send brpc when exchange"),
                ErrorCode.ERR_FAILED_TO_SEND_BRPC_HOST_DOWN);
        errorMap.put(Pattern.compile("hyperscan"), ErrorCode.ERR_HYPERSCAN_ERROR);
        errorMap.put(Pattern.compile("Cancelled"), ErrorCode.ERR_CANCELLED);
        errorMap.put(Pattern.compile("Orc row reader nextBatch failed"),
                ErrorCode.ERR_ORC_ROW_READER_NEXTBATCH_FAILED);
        errorMap.put(Pattern.compile("FullGC release wg overcommit mem"),
                ErrorCode.ERR_FULLGC_RELEASE_OVERCOMMIT_MEM);
        errorMap.put(Pattern.compile("Can not build QueryTableValuedFunction by query"),
                ErrorCode.ERR_CANNOT_BUILD_QUERY_TABLE_VALUED_FUNCTION);
        errorMap.put(Pattern.compile("Syntax error"), ErrorCode.ERR_SYNTAX_ERROR);
        errorMap.put(Pattern.compile("Unmatched string literal"), ErrorCode.ERR_UMATCHED_STRING_LITERAL);
        errorMap.put(Pattern.compile("Incomplete escape sequence"), ErrorCode.ERR_ILLEGAL_STATE_EXCEPTION);
        errorMap.put(Pattern.compile("user cancel"), ErrorCode.ERR_USER_CANCELED);
        errorMap.put(Pattern.compile("timeout when waiting for send fragments rpc"),
                ErrorCode.ERR_SEND_FRAGMENTS_FAILED);
        errorMap.put(Pattern.compile("Process memory not enough"),
                ErrorCode.ERR_PROCESS_MEMORY_NOT_ENOUGH_CANCEL_QUERY);
        errorMap.put(Pattern.compile("Query may be timeout or be cancelled"),
                ErrorCode.ERR_USER_CANCELED);
        errorMap.put(Pattern.compile("query timeout"), ErrorCode.ERR_EXECUTE_TIMEOUT);
        errorMap.put(Pattern.compile("Allocator sys memory check failed"),
                ErrorCode.ERR_CREATE_EXPR_MEM_ALLOC_FAILED);
        errorMap.put(Pattern.compile("Table .* does not exist in database"),
                ErrorCode.ERR_TABLE_DOES_NOT_EXIST_IN_DATABASE);
        errorMap.put(Pattern.compile("failed to get table .*NoSuchObjectException.*\\$partitions table not found"),
                ErrorCode.ERR_TABLE_PARTITIONS_TABLE_NOT_FOUND);
        errorMap.put(Pattern.compile("failed to get table.*NoSuchObjectException.*table not found"),
                ErrorCode.ERR_NO_SUCH_OBJECT);
        errorMap.put(Pattern.compile("Only support csv data in utf8 codec"),
                ErrorCode.ERR_ONLY_SUPPORT_CSV_DATA_IN_UTF8_CODEC);
        errorMap.put(Pattern.compile("please check your sql"),
                ErrorCode.ERR_PLEASE_CHECK_YOUR_SQL);
        errorMap.put(Pattern.compile("External catalog .* is not allowed in 'DescribeStmt ALL'"),
                ErrorCode.ERR_IS_NOT_ALLOWED_IN_DESCRIBE_STMT_ALL);
        errorMap.put(Pattern.compile("Failed to create orc row reader"),
                ErrorCode.ERR_FAILED_TO_CREATE_ORC_ROW_READER);
        errorMap.put(Pattern.compile("ParseException"),
                ErrorCode.ERR_PARSE_EXCEPTION);
        errorMap.put(Pattern.compile("failed to get table.*MetaException.*权限"),
                ErrorCode.ERR_MetaException);
    }

    public static ErrorCode getErrorCode(String errorMessage) {
        for (Map.Entry<Pattern, ErrorCode> entry : errorMap.entrySet()) {
            if (entry.getKey().matcher(errorMessage).find()) {
                return entry.getValue();
            }
        }
        return ErrorCode.ERR_UNKNOWN_ERROR;
    }
}




