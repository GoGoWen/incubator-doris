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

#include "vec/exprs/vcompound_pred.h"

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "common/status.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Opcodes_types.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/exprs/vexpr_context.h"
#include "exprs/mock_vexpr.h"

using ::testing::_;
using ::testing::DoAll;
using ::testing::Return;
using ::testing::SetArgPointee;

namespace doris::vectorized {

class VCompoundPredTest : public testing::Test {
protected:
    void SetUp() override {
        // Create a basic TExprNode for VCompoundPred
        _node.node_type = TExprNodeType::COMPOUND_PRED;
        _node.type.types.resize(1);
        _node.type.types[0].__set_type(TTypeNodeType::SCALAR);
        _node.type.types[0].__set_scalar_type(TScalarType());
        _node.type.types[0].scalar_type.__set_type(TPrimitiveType::BOOLEAN);
        _node.num_children = 2;
        _node.is_nullable = false;
    }

    void TearDown() override {
        _children.clear();
    }

    // Helper function to create a boolean column with given values
    ColumnPtr create_bool_column(const std::vector<uint8_t>& values, bool nullable = false) {
        auto column = ColumnUInt8::create();
        for (auto val : values) {
            column->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
        }

        if (nullable) {
            auto null_map = ColumnUInt8::create(values.size(), 0);
            return ColumnNullable::create(std::move(column), std::move(null_map));
        }
        return column;
    }

    // Helper function to create a nullable boolean column with null values
    ColumnPtr create_nullable_bool_column(const std::vector<uint8_t>& values,
                                         const std::vector<uint8_t>& null_map) {
        auto column = ColumnUInt8::create();
        for (auto val : values) {
            column->insert_data(reinterpret_cast<const char*>(&val), sizeof(val));
        }

        auto null_column = ColumnUInt8::create();
        for (auto null_val : null_map) {
            null_column->insert_data(reinterpret_cast<const char*>(&null_val), sizeof(null_val));
        }

        return ColumnNullable::create(std::move(column), std::move(null_column));
    }

    // Helper function to get uint value from column (handles nullable columns)
    uint8_t get_column_uint_value(const ColumnPtr& column, size_t index) {
        if (column->is_nullable()) {
            auto nullable_column = assert_cast<const ColumnNullable*>(column.get());
            return nullable_column->get_nested_column().get_uint(index);
        } else {
            return column->get_uint(index);
        }
    }

    // Helper function to create mock children that return specific columns
    void setup_mock_children(ColumnPtr lhs_column, ColumnPtr rhs_column) {
        _children.clear();

        auto lhs_child = std::make_shared<MockVExpr>();
        auto rhs_child = std::make_shared<MockVExpr>();

        EXPECT_CALL(*lhs_child, execute(_, _, _))
            .WillRepeatedly(DoAll(SetArgPointee<2>(0), Return(Status::OK())));
        EXPECT_CALL(*rhs_child, execute(_, _, _))
            .WillRepeatedly(DoAll(SetArgPointee<2>(1), Return(Status::OK())));
        EXPECT_CALL(*lhs_child, is_compound_predicate())
            .WillRepeatedly(Return(false));
        EXPECT_CALL(*rhs_child, is_compound_predicate())
            .WillRepeatedly(Return(false));
        EXPECT_CALL(*lhs_child, is_constant())
            .WillRepeatedly(Return(false));
        EXPECT_CALL(*rhs_child, is_constant())
            .WillRepeatedly(Return(false));

        _children.push_back(lhs_child);
        _children.push_back(rhs_child);
    }

    TExprNode _node;
    std::vector<std::shared_ptr<MockVExpr>> _children;
};

TEST_F(VCompoundPredTest, TestAndOperation_BothTrue) {
    // Test: true AND true = true
    _node.opcode = TExprOpcode::COMPOUND_AND;

    auto lhs_column = create_bool_column({1, 1, 1});  // all true
    auto rhs_column = create_bool_column({1, 1, 1});  // all true

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);

    // All results should be true (1)
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), 1);
    }
}

TEST_F(VCompoundPredTest, TestAndOperation_BothFalse) {
    // Test: false AND false = false
    _node.opcode = TExprOpcode::COMPOUND_AND;

    auto lhs_column = create_bool_column({0, 0, 0});  // all false
    auto rhs_column = create_bool_column({0, 0, 0});  // all false

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);

    // All results should be false (0)
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), 0);
    }
}

TEST_F(VCompoundPredTest, TestAndOperation_Mixed) {
    // Test: mixed values AND operation
    _node.opcode = TExprOpcode::COMPOUND_AND;

    auto lhs_column = create_bool_column({1, 0, 1, 0});  // true, false, true, false
    auto rhs_column = create_bool_column({1, 1, 0, 0});  // true, true, false, false

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 4);

    // Expected results: true&true=true, false&true=false, true&false=false, false&false=false
    std::vector<uint8_t> expected = {1, 0, 0, 0};
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), expected[i]);
    }
}

TEST_F(VCompoundPredTest, TestOrOperation_BothTrue) {
    // Test: true OR true = true
    _node.opcode = TExprOpcode::COMPOUND_OR;

    auto lhs_column = create_bool_column({1, 1, 1});  // all true
    auto rhs_column = create_bool_column({1, 1, 1});  // all true

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);

    // All results should be true (1)
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), 1);
    }
}

TEST_F(VCompoundPredTest, TestOrOperation_Mixed) {
    // Test: mixed values OR operation
    _node.opcode = TExprOpcode::COMPOUND_OR;

    auto lhs_column = create_bool_column({1, 0, 1, 0});  // true, false, true, false
    auto rhs_column = create_bool_column({1, 1, 0, 0});  // true, true, false, false

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 4);

    // Expected results: true|true=true, false|true=true, true|false=true, false|false=false
    std::vector<uint8_t> expected = {1, 1, 1, 0};
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), expected[i]);
    }
}

TEST_F(VCompoundPredTest, TestAndOperation_LeftAllFalse_Optimization) {
    // Test optimization: false AND any = false (should return left column)
    _node.opcode = TExprOpcode::COMPOUND_AND;

    auto lhs_column = create_bool_column({0, 0, 0});  // all false
    auto rhs_column = create_bool_column({1, 0, 1});  // mixed values

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);

    // All results should be false (0) due to optimization
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), 0);
    }
}

TEST_F(VCompoundPredTest, TestOrOperation_LeftAllTrue_Optimization) {
    // Test optimization: true OR any = true (should return left column)
    _node.opcode = TExprOpcode::COMPOUND_OR;

    auto lhs_column = create_bool_column({1, 1, 1});  // all true
    auto rhs_column = create_bool_column({1, 0, 1});  // mixed values

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);

    // All results should be true (1) due to optimization
    for (size_t i = 0; i < result_column->size(); ++i) {
        EXPECT_EQ(get_column_uint_value(result_column, i), 1);
    }
}

TEST_F(VCompoundPredTest, TestAndOperation_WithNullable) {
    // Test AND operation with nullable columns
    _node.opcode = TExprOpcode::COMPOUND_AND;
    _node.is_nullable = true;

    // Create nullable columns: [true, false, null] AND [true, true, false]
    auto lhs_column = create_nullable_bool_column({1, 0, 0}, {0, 0, 1});  // true, false, null
    auto rhs_column = create_nullable_bool_column({1, 1, 0}, {0, 0, 0});  // true, true, false

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    pred._data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);
    EXPECT_TRUE(result_column->is_nullable());

    auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
    auto& data_column = nullable_column->get_nested_column();
    auto& null_map = nullable_column->get_null_map_column();

    // Expected results: true&true=true, false&true=false, null&false=false
    EXPECT_EQ(data_column.get_uint(0), 1);  // true
    EXPECT_EQ(data_column.get_uint(1), 0);  // false
    EXPECT_EQ(data_column.get_uint(2), 0);  // false (null & false = false)

    EXPECT_EQ(null_map.get_uint(0), 0);  // not null
    EXPECT_EQ(null_map.get_uint(1), 0);  // not null
    EXPECT_EQ(null_map.get_uint(2), 0);  // not null (null & false = false, not null)
}

TEST_F(VCompoundPredTest, TestOrOperation_WithNullable) {
    // Test OR operation with nullable columns
    _node.opcode = TExprOpcode::COMPOUND_OR;
    _node.is_nullable = true;

    // Create nullable columns: [false, true, null] OR [false, false, true]
    auto lhs_column = create_nullable_bool_column({0, 1, 0}, {0, 0, 1});  // false, true, null
    auto rhs_column = create_nullable_bool_column({0, 0, 1}, {0, 0, 0});  // false, false, true

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    pred._data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 3);
    EXPECT_TRUE(result_column->is_nullable());

    auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
    auto& data_column = nullable_column->get_nested_column();
    auto& null_map = nullable_column->get_null_map_column();

    // Expected results: false|false=false, true|false=true, null|true=true
    EXPECT_EQ(data_column.get_uint(0), 0);  // false
    EXPECT_EQ(data_column.get_uint(1), 1);  // true
    EXPECT_EQ(data_column.get_uint(2), 1);  // true (null | true = true)

    EXPECT_EQ(null_map.get_uint(0), 0);  // not null
    EXPECT_EQ(null_map.get_uint(1), 0);  // not null
    EXPECT_EQ(null_map.get_uint(2), 0);  // not null (null | true = true, not null)
}

TEST_F(VCompoundPredTest, TestNullAndNull) {
    // Test null AND null = null
    _node.opcode = TExprOpcode::COMPOUND_AND;
    _node.is_nullable = true;

    // Create nullable columns: [null, null] AND [null, null]
    auto lhs_column = create_nullable_bool_column({0, 0}, {1, 1});  // null, null
    auto rhs_column = create_nullable_bool_column({0, 0}, {1, 1});  // null, null

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    pred._data_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>());
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_TRUE(status.ok());
    EXPECT_GE(result_column_id, 0);

    auto result_column = block.get_by_position(result_column_id).column;
    EXPECT_EQ(result_column->size(), 2);
    EXPECT_TRUE(result_column->is_nullable());

    auto nullable_column = assert_cast<const ColumnNullable*>(result_column.get());
    auto& null_map = nullable_column->get_null_map_column();

    // Both results should be null
    EXPECT_EQ(null_map.get_uint(0), 1);  // null
    EXPECT_EQ(null_map.get_uint(1), 1);  // null
}

TEST_F(VCompoundPredTest, TestInvalidOpcode) {
    // Test invalid opcode should return error
    _node.opcode = TExprOpcode::INVALID_OPCODE;  // Invalid opcode

    auto lhs_column = create_bool_column({1, 0});
    auto rhs_column = create_bool_column({0, 1});

    setup_mock_children(lhs_column, rhs_column);

    VCompoundPred pred(_node);
    for (auto child : _children) {
        pred.add_child(child);
    }

    Block block;
    block.insert({lhs_column, std::make_shared<DataTypeUInt8>(), "lhs"});
    block.insert({rhs_column, std::make_shared<DataTypeUInt8>(), "rhs"});

    int result_column_id = -1;
    Status status = pred.execute(nullptr, &block, &result_column_id);

    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.code(), ErrorCode::INTERNAL_ERROR);
}
} // namespace doris::vectorized