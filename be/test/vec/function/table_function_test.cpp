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

#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gmock/gmock-spec-builders.h>
#include <gtest/gtest-matchers.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/mock_vexpr.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/runtime_state.h"
#include "testutil/any_type.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/columns/column_struct.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/table_function/vexplode.h"
#include "vec/exprs/table_function/vexplode_numbers.h"
#include "vec/exprs/table_function/vexplode_split.h"
#include "vec/exprs/table_function/vposexplode.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/function/function_test_util.h"

namespace doris::vectorized {

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

class TableFunctionTest : public testing::Test {
protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

    void clear() {
        _ctx = nullptr;
        _root = nullptr;
        _children.clear();
        _column_ids.clear();
    }

    void init_expr_context(int child_num) {
        clear();

        _root = std::make_shared<MockVExpr>();
        for (int i = 0; i < child_num; ++i) {
            _column_ids.push_back(i);
            _children.push_back(std::make_shared<MockVExpr>());
            EXPECT_CALL(*_children[i], execute(_, _, _))
                    .WillRepeatedly(DoAll(SetArgPointee<2>(_column_ids[i]), Return(Status::OK())));
            _root->add_child(_children[i]);
        }
        _ctx = std::make_shared<VExprContext>(_root);
    }

private:
    VExprContextSPtr _ctx;
    std::shared_ptr<MockVExpr> _root;
    std::vector<std::shared_ptr<MockVExpr>> _children;
    std::vector<int> _column_ids;
};

TEST_F(TableFunctionTest, vexplode_outer) {
    init_expr_context(1);
    VExplodeTableFunction explode_outer;
    explode_outer.set_outer();
    explode_outer.set_expr_context(_ctx);

    // explode_outer(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{vec}, {Null()}, {Array()}};

        InputTypeSet output_types = {TypeIndex::Int32};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)},
                                   {Int32(3)}, {Null()}, {Null()}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};
        Array vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::String};
        InputDataSet output_set = {
                {Null()}, {Null()}, {std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }

    // explode_outer(Array<Decimal>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Decimal128V2};
        Array vec = {ut_type::DECIMALFIELD(17014116.67), ut_type::DECIMALFIELD(-17014116.67)};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::Decimal128V2};
        InputDataSet output_set = {{Null()},
                                   {Null()},
                                   {ut_type::DECIMAL(17014116.67)},
                                   {ut_type::DECIMAL(-17014116.67)}};

        check_vec_table_function(&explode_outer, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode) {
    init_expr_context(1);
    VExplodeTableFunction explode;
    explode.set_expr_context(_ctx);

    // explode(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};

        Array vec = {Int32(1), Null(), Int32(2), Int32(3)};
        InputDataSet input_set = {{vec}, {Null()}, {Array()}};

        InputTypeSet output_types = {TypeIndex::Int32};
        InputDataSet output_set = {{Int32(1)}, {Null()}, {Int32(2)}, {Int32(3)}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }

    // explode(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};
        Array vec = {std::string("abc"), std::string(""), std::string("def")};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::String};
        InputDataSet output_set = {{std::string("abc")}, {std::string("")}, {std::string("def")}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }

    // explode(Array<Date>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Date};
        Array vec = {Null(), str_to_date_time("2022-01-02", false)};
        InputDataSet input_set = {{Null()}, {Array()}, {vec}};

        InputTypeSet output_types = {TypeIndex::Date};
        InputDataSet output_set = {{Null()}, {std::string("2022-01-02")}};

        check_vec_table_function(&explode, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_numbers) {
    init_expr_context(1);
    VExplodeNumbersTableFunction tfn;
    tfn.set_expr_context(_ctx);

    {
        InputTypeSet input_types = {TypeIndex::Int32};
        InputDataSet input_set = {{Int32(2)}, {Int32(3)}, {Null()}, {Int32(0)}, {Int32(-2)}};

        InputTypeSet output_types = {TypeIndex::Int32};
        InputDataSet output_set = {{Int32(0)}, {Int32(1)}, {Int32(0)}, {Int32(1)}, {Int32(2)}};

        check_vec_table_function(&tfn, input_types, input_set, output_types, output_set);
    }
}

TEST_F(TableFunctionTest, vexplode_split) {
    init_expr_context(2);
    VExplodeSplitTableFunction tfn;
    tfn.set_expr_context(_ctx);

    {
        // Case 1: explode_split(null) --- null
        // Case 2: explode_split("a,b,c", ",") --> ["a", "b", "c"]
        // Case 3: explode_split("a,b,c", "a,")) --> ["", "b,c"]
        // Case 4: explode_split("", ",")) --> [""]
        InputTypeSet input_types = {TypeIndex::String, Consted {TypeIndex::String}};
        InputDataSet input_sets = {{std::string("a,b,c"), std::string(",")},
                                   {std::string("a,b,c"), std::string("a,")},
                                   {std::string(""), std::string(",")}};

        InputTypeSet output_types = {TypeIndex::String};
        InputDataSet output_sets = {{std::string("a"), std::string("b"), std::string("c")},
                                    {std::string(""), std::string("b,c")},
                                    {std::string("")}};

        for (int i = 0; i < input_sets.size(); ++i) {
            InputDataSet input_set {input_sets[i]};
            InputDataSet output_set {};
            for (const auto& data : output_sets[i]) {
                output_set.emplace_back(std::vector<AnyType> {data});
            }
            check_vec_table_function(&tfn, input_types, input_set, output_types, output_set);
        }
    }
}

// Custom test helper for posexplode since it returns struct with two columns
Block* process_posexplode_table_function(VPosExplodeTableFunction* fn, Block* input_block) {
    RuntimeState runtime_state((TQueryGlobals()));

    // process table function init
    if (fn->process_init(input_block, &runtime_state) != Status::OK()) {
        LOG(WARNING) << "VPosExplodeTableFunction process_init failed";
        return nullptr;
    }

    // Create struct data type for output (position: Int32, value: depends on input array type)
    DataTypes struct_types;
    struct_types.push_back(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt32>())); // position

    // Get the array element type from input
    auto input_column = input_block->get_by_position(0);
    const DataTypeArray* array_type = nullptr;

    // Handle nullable array type
    if (input_column.type->is_nullable()) {
        auto nullable_type = assert_cast<const DataTypeNullable*>(input_column.type.get());
        array_type = assert_cast<const DataTypeArray*>(nullable_type->get_nested_type().get());
    } else {
        array_type = assert_cast<const DataTypeArray*>(input_column.type.get());
    }

    struct_types.push_back(array_type->get_nested_type()); // value

    auto struct_type = std::make_shared<DataTypeStruct>(struct_types);
    auto nullable_struct_type = std::make_shared<DataTypeNullable>(struct_type);
    vectorized::MutableColumnPtr column = nullable_struct_type->create_column();
    fn->set_nullable();

    // process table function for all rows
    for (size_t row = 0; row < input_block->rows(); ++row) {
        fn->process_row(row);

        // consider outer
        if (!fn->is_outer() && fn->current_empty()) {
            continue;
        }

        do {
            fn->get_same_many_values(column, 1);
            fn->forward();
        } while (!fn->eos());
    }

    std::unique_ptr<Block> output_block = Block::create_unique();
    output_block->insert({std::move(column), nullable_struct_type, "posexplode_result"});
    return output_block.release();
}

TEST_F(TableFunctionTest, vposexplode) {
    init_expr_context(1);
    VPosExplodeTableFunction posexplode;
    posexplode.set_expr_context(_ctx);

    // posexplode(Array<Int32>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array vec = {Int32(10), Int32(20), Int32(30)};
        InputDataSet input_set = {{vec}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 3);

        // Verify the nullable struct column
        auto& result_column = output_block->get_by_position(0).column;
        EXPECT_TRUE(result_column->is_nullable());

        auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
        auto& nested_column = nullable_column->get_nested_column();
        EXPECT_TRUE(nested_column.is_column_struct());

        auto struct_column = assert_cast<const ColumnStruct*>(&nested_column);
        EXPECT_EQ(struct_column->tuple_size(), 2);

        // Check position column (first element of struct)
        auto& pos_column = struct_column->get_column(0);
        EXPECT_TRUE(pos_column.is_nullable());
        auto pos_nullable = assert_cast<const ColumnNullable*>(&pos_column);
        auto pos_data = assert_cast<const ColumnInt32*>(&pos_nullable->get_nested_column());

        EXPECT_EQ(pos_data->get_data()[0], 0);
        EXPECT_EQ(pos_data->get_data()[1], 1);
        EXPECT_EQ(pos_data->get_data()[2], 2);

        // Check value column (second element of struct)
        auto& val_column = struct_column->get_column(1);
        auto val_data = assert_cast<const ColumnInt32*>(&val_column);

        EXPECT_EQ(val_data->get_data()[0], 10);
        EXPECT_EQ(val_data->get_data()[1], 20);
        EXPECT_EQ(val_data->get_data()[2], 30);
    }

    // posexplode(Array<String>)
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::String};
        Array vec = {std::string("hello"), std::string("world")};
        InputDataSet input_set = {{vec}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 2);

        auto& result_column = output_block->get_by_position(0).column;
        auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
        auto struct_column = assert_cast<const ColumnStruct*>(&nullable_column->get_nested_column());

        // Check positions
        auto& pos_column = struct_column->get_column(0);
        auto pos_nullable = assert_cast<const ColumnNullable*>(&pos_column);
        auto pos_data = assert_cast<const ColumnInt32*>(&pos_nullable->get_nested_column());

        EXPECT_EQ(pos_data->get_data()[0], 0);
        EXPECT_EQ(pos_data->get_data()[1], 1);

        // Check values
        auto& val_column = struct_column->get_column(1);
        auto val_data = assert_cast<const ColumnString*>(&val_column);

        EXPECT_EQ(val_data->get_data_at(0).to_string(), "hello");
        EXPECT_EQ(val_data->get_data_at(1).to_string(), "world");
    }

    // posexplode with empty array
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array empty_vec = {};
        InputDataSet input_set = {{empty_vec}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 0); // Empty array should produce no rows
    }

    // posexplode with null array
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        InputDataSet input_set = {{Null()}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 0); // Null array should produce no rows
    }

    // posexplode with array containing null elements
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array vec = {Int32(1), Null(), Int32(3)};
        InputDataSet input_set = {{vec}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 3);

        auto& result_column = output_block->get_by_position(0).column;
        auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
        auto struct_column = assert_cast<const ColumnStruct*>(&nullable_column->get_nested_column());

        // Check positions
        auto& pos_column = struct_column->get_column(0);
        auto pos_nullable = assert_cast<const ColumnNullable*>(&pos_column);
        auto pos_data = assert_cast<const ColumnInt32*>(&pos_nullable->get_nested_column());

        EXPECT_EQ(pos_data->get_data()[0], 0);
        EXPECT_EQ(pos_data->get_data()[1], 1);
        EXPECT_EQ(pos_data->get_data()[2], 2);

        // Check values - second element should be null
        auto& val_column = struct_column->get_column(1);
        if (val_column.is_nullable()) {
            auto val_nullable = assert_cast<const ColumnNullable*>(&val_column);
            auto& null_map = val_nullable->get_null_map_data();
            EXPECT_FALSE(null_map[0]); // first element not null
            EXPECT_TRUE(null_map[1]);  // second element is null
            EXPECT_FALSE(null_map[2]); // third element not null

            auto val_data = assert_cast<const ColumnInt32*>(&val_nullable->get_nested_column());
            EXPECT_EQ(val_data->get_data()[0], 1);
            EXPECT_EQ(val_data->get_data()[2], 3);
        }
    }
}

TEST_F(TableFunctionTest, vposexplode_outer) {
    init_expr_context(1);
    VPosExplodeTableFunction posexplode_outer;
    posexplode_outer.set_outer();
    posexplode_outer.set_expr_context(_ctx);

    // posexplode_outer with empty array should produce one row with default values
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array empty_vec = {};
        InputDataSet input_set = {{empty_vec}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode_outer, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 1); // Empty array should produce one row in outer mode
    }

    // posexplode_outer with null array should produce one row with default values
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        InputDataSet input_set = {{Null()}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode_outer, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 1); // Null array should produce one row in outer mode
    }

    // posexplode_outer with normal array should work like regular posexplode
    {
        InputTypeSet input_types = {TypeIndex::Array, TypeIndex::Int32};
        Array vec = {Int32(100), Int32(200)};
        InputDataSet input_set = {{vec}};

        std::unique_ptr<Block> input_block(create_block_from_inputset(input_types, input_set));
        EXPECT_TRUE(input_block != nullptr);

        std::unique_ptr<Block> output_block(process_posexplode_table_function(&posexplode_outer, input_block.get()));
        EXPECT_TRUE(output_block != nullptr);
        EXPECT_EQ(output_block->rows(), 2);

        auto& result_column = output_block->get_by_position(0).column;
        auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
        auto struct_column = assert_cast<const ColumnStruct*>(&nullable_column->get_nested_column());

        // Check positions
        auto& pos_column = struct_column->get_column(0);
        auto pos_nullable = assert_cast<const ColumnNullable*>(&pos_column);
        auto pos_data = assert_cast<const ColumnInt32*>(&pos_nullable->get_nested_column());

        EXPECT_EQ(pos_data->get_data()[0], 0);
        EXPECT_EQ(pos_data->get_data()[1], 1);

        // Check values
        auto& val_column = struct_column->get_column(1);
        auto val_data = assert_cast<const ColumnInt32*>(&val_column);

        EXPECT_EQ(val_data->get_data()[0], 100);
        EXPECT_EQ(val_data->get_data()[1], 200);
    }
}

} // namespace doris::vectorized

