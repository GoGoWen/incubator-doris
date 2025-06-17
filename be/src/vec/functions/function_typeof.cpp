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

#include "vec/functions/function_typeof.h"

#include <algorithm>
#include <cctype>

#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

DataTypePtr FunctionTypeOf::get_return_type_impl(const DataTypes& arguments) const {
    return std::make_shared<DataTypeString>();
}

Status FunctionTypeOf::execute_impl(FunctionContext* context, Block& block,
                                    const ColumnNumbers& arguments, size_t result,
                                    size_t input_rows_count) const {
    const auto& argument_column = block.get_by_position(arguments[0]);
    const auto& argument_type = argument_column.type;

    std::string type_name = argument_type->get_name();

    if (type_name.find("Nullable(") == 0) {
        // Extract inner type from "Nullable(InnerType)"
        size_t start = type_name.find('(') + 1;
        size_t end = type_name.find_last_of(')');
        if (start < end) {
            type_name = type_name.substr(start, end - start);
        }
    }

    if (type_name == "Int8") {
        type_name = "tinyint";
    } else if (type_name == "Int16") {
        type_name = "smallint";
    } else if (type_name == "Int32") {
        type_name = "int";
    } else if (type_name == "Int64") {
        type_name = "bigint";
    } else if (type_name == "Int128") {
        type_name = "largeint";
    } else if (type_name == "UInt8") {
        type_name = "boolean";  // UInt8 is used for boolean in Doris
    } else if (type_name == "Float32") {
        type_name = "float";
    } else if (type_name == "Float64") {
        type_name = "double";
    } else if (type_name == "String") {
        type_name = "varchar";
    } else if (type_name == "Date") {
        type_name = "date";
    } else if (type_name == "DateV2") {
        type_name = "datev2";
    } else if (type_name == "Datetime") {
        type_name = "datetime";
    } else if (type_name == "DatetimeV2") {
        type_name = "datetimev2";
    } else if (type_name.find("Decimal") == 0) {
        std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::tolower);
        size_t pos = 0;
        while ((pos = type_name.find(", ", pos)) != std::string::npos) {
            type_name.replace(pos, 2, ",");
            pos += 1;
        }
    } else if (type_name == "Nothing") {
        type_name = "null";
    } else {
        std::transform(type_name.begin(), type_name.end(), type_name.begin(), ::tolower);
    }

    auto return_type = get_return_type_impl({argument_type});
    block.get_by_position(result).column = return_type->create_column_const(
        input_rows_count, Field(type_name));

    return Status::OK();
}

void register_function_typeof(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionTypeOf>();
}

} // namespace doris::vectorized
