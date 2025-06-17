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

#include <gtest/gtest.h>

#include "vec/columns/column_const.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_date.h"
#include "vec/data_types/data_type_date_time.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_typeof.h"
#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

class FunctionTypeOfTest : public testing::Test {
public:
    void SetUp() override {
        function = FunctionTypeOf::create();
    }

protected:
    FunctionPtr function;

    template <typename DataType>
    void test_basic_type(const std::string& expected_type_name) {
        auto data_type = std::make_shared<DataType>();
        auto column = data_type->create_column_const(1, data_type->get_default());
        
        Block block;
        block.insert({column, data_type, "test_col"});
        block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});
        
        ColumnNumbers arguments = {0};
        size_t result_pos = 1;
        
        ASSERT_TRUE(function->execute_impl(nullptr, block, arguments, result_pos, 1).ok());
        
        auto result_column = block.get_by_position(result_pos).column;
        auto const_column = assert_cast<const ColumnConst*>(result_column.get());
        auto string_column = assert_cast<const ColumnString*>(&const_column->get_data_column());
        
        ASSERT_EQ(string_column->get_data_at(0).to_string(), expected_type_name);
    }

    template <typename DataType>
    void test_nullable_type(const std::string& expected_type_name) {
        auto inner_type = std::make_shared<DataType>();
        auto nullable_type = std::make_shared<DataTypeNullable>(inner_type);
        auto column = nullable_type->create_column_const(1, Field());
        
        Block block;
        block.insert({column, nullable_type, "test_col"});
        block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});
        
        ColumnNumbers arguments = {0};
        size_t result_pos = 1;
        
        ASSERT_TRUE(function->execute_impl(nullptr, block, arguments, result_pos, 1).ok());
        
        auto result_column = block.get_by_position(result_pos).column;
        auto const_column = assert_cast<const ColumnConst*>(result_column.get());
        auto string_column = assert_cast<const ColumnString*>(&const_column->get_data_column());
        
        ASSERT_EQ(string_column->get_data_at(0).to_string(), expected_type_name);
    }
};

TEST_F(FunctionTypeOfTest, TestBasicIntegerTypes) {
    test_basic_type<DataTypeInt8>("tinyint");
    test_basic_type<DataTypeInt16>("smallint");
    test_basic_type<DataTypeInt32>("int");
    test_basic_type<DataTypeInt64>("bigint");
    test_basic_type<DataTypeInt128>("largeint");
}

TEST_F(FunctionTypeOfTest, TestBasicFloatingTypes) {
    test_basic_type<DataTypeFloat32>("float");
    test_basic_type<DataTypeFloat64>("double");
}

TEST_F(FunctionTypeOfTest, TestStringTypes) {
    test_basic_type<DataTypeString>("varchar");
}

TEST_F(FunctionTypeOfTest, TestBooleanType) {
    test_basic_type<DataTypeUInt8>("boolean");
}

TEST_F(FunctionTypeOfTest, TestDateTimeTypes) {
    test_basic_type<DataTypeDate>("date");
    test_basic_type<DataTypeDateTime>("datetime");
}

TEST_F(FunctionTypeOfTest, TestDecimalTypes) {
    // Test with a simple decimal type - we'll test the actual decimal formatting in integration tests
    // For unit tests, we just verify the function doesn't crash with decimal types
    auto decimal_type = std::make_shared<DataTypeDecimal<Decimal64>>(10, 2);
    auto column = decimal_type->create_column_const(1, decimal_type->get_default());

    Block block;
    block.insert({column, decimal_type, "test_col"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

    ColumnNumbers arguments = {0};
    size_t result_pos = 1;

    ASSERT_TRUE(function->execute_impl(nullptr, block, arguments, result_pos, 1).ok());

    auto result_column = block.get_by_position(result_pos).column;
    auto const_column = assert_cast<const ColumnConst*>(result_column.get());
    auto string_column = assert_cast<const ColumnString*>(&const_column->get_data_column());

    // Should return some form of decimal type name
    std::string result = string_column->get_data_at(0).to_string();
    ASSERT_TRUE(result.find("decimal") != std::string::npos);
}

TEST_F(FunctionTypeOfTest, TestNullableTypes) {
    // Test nullable int type
    auto inner_type = std::make_shared<DataTypeInt32>();
    auto nullable_type = std::make_shared<DataTypeNullable>(inner_type);
    auto column = nullable_type->create_column_const(1, Field());

    Block block;
    block.insert({column, nullable_type, "test_col"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});

    ColumnNumbers arguments = {0};
    size_t result_pos = 1;

    ASSERT_TRUE(function->execute_impl(nullptr, block, arguments, result_pos, 1).ok());

    auto result_column = block.get_by_position(result_pos).column;
    auto const_column = assert_cast<const ColumnConst*>(result_column.get());
    auto string_column = assert_cast<const ColumnString*>(&const_column->get_data_column());

    // Should return the inner type name, not "nullable(int)"
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "int");
}

TEST_F(FunctionTypeOfTest, TestMultipleRows) {
    auto data_type = std::make_shared<DataTypeInt32>();
    auto column = data_type->create_column_const(5, Field(static_cast<Int32>(42)));
    
    Block block;
    block.insert({column, data_type, "test_col"});
    block.insert({nullptr, std::make_shared<DataTypeString>(), "result"});
    
    ColumnNumbers arguments = {0};
    size_t result_pos = 1;
    
    ASSERT_TRUE(function->execute_impl(nullptr, block, arguments, result_pos, 5).ok());
    
    auto result_column = block.get_by_position(result_pos).column;
    auto const_column = assert_cast<const ColumnConst*>(result_column.get());
    
    ASSERT_EQ(const_column->size(), 5);
    
    auto string_column = assert_cast<const ColumnString*>(&const_column->get_data_column());
    ASSERT_EQ(string_column->get_data_at(0).to_string(), "int");
}

TEST_F(FunctionTypeOfTest, TestReturnType) {
    DataTypes argument_types = {std::make_shared<DataTypeInt32>()};
    auto return_type = function->get_return_type_impl(argument_types);
    
    ASSERT_TRUE(return_type->equals(*std::make_shared<DataTypeString>()));
}

TEST_F(FunctionTypeOfTest, TestFunctionProperties) {
    ASSERT_EQ(function->get_name(), "typeof");
    ASSERT_EQ(function->get_number_of_arguments(), 1);
    ASSERT_FALSE(function->use_default_implementation_for_nulls());
}

} // namespace doris::vectorized
