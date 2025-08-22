#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <string>

#include "gtest/gtest_pred_impl.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/data_types/data_type_decimal.h"

using namespace doris::vectorized;

namespace doris::vectorized {
// forward declarations for registration
void register_aggregate_function_sum(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_avg(AggregateFunctionSimpleFactory& factory);
void register_aggregate_function_combinator_distinct(AggregateFunctionSimpleFactory& factory);
} // namespace doris::vectorized

namespace {

// trim helpers
static inline std::string trim(const std::string& s) {
    size_t l = 0;
    while (l < s.size() && isspace(static_cast<unsigned char>(s[l]))) ++l;
    size_t r = s.size();
    while (r > l && isspace(static_cast<unsigned char>(s[r - 1]))) --r;
    return s.substr(l, r - l);
}

static int extract_precision(const std::string& type_name) {
    auto start = type_name.find("Decimal(");
    if (start == std::string::npos) return -1;
    start += std::string("Decimal(").size();
    auto comma = type_name.find(',', start);
    if (comma == std::string::npos) return -1;
    auto prec_str = trim(type_name.substr(start, comma - start));
    return std::stoi(prec_str);
}

AggregateFunctionSimpleFactory make_factory() {
    AggregateFunctionSimpleFactory factory;
    register_aggregate_function_sum(factory);
    register_aggregate_function_avg(factory);
    register_aggregate_function_combinator_distinct(factory);
    return factory;
}

TEST(AggFactoryDecimal256RoutingTest, MultiDistinctSumDecimal256Routing) {
    auto factory = make_factory();
    DataTypePtr dec38_12 = std::make_shared<DataTypeDecimal<Decimal128V3>>(38, 12);
    DataTypes args {dec38_12};

    auto f_no_flag = factory.get("multi_distinct_sum", args, false, 100, false);
    ASSERT_NE(nullptr, f_no_flag);
    EXPECT_EQ(38, extract_precision(f_no_flag->get_return_type()->get_name()));

    auto f_flag = factory.get("multi_distinct_sum", args, false, 100, true);
    ASSERT_NE(nullptr, f_flag);
    EXPECT_EQ(76, extract_precision(f_flag->get_return_type()->get_name()));
}

TEST(AggFactoryDecimal256RoutingTest, MultiDistinctAvgDecimal256Routing) {
    auto factory = make_factory();
    DataTypePtr dec38_12 = std::make_shared<DataTypeDecimal<Decimal128V3>>(38, 12);
    DataTypes args {dec38_12};

    auto f_no_flag = factory.get("multi_distinct_avg", args, false, 100, false);
    ASSERT_NE(nullptr, f_no_flag);
    EXPECT_EQ(38, extract_precision(f_no_flag->get_return_type()->get_name()));

    auto f_flag = factory.get("multi_distinct_avg", args, false, 100, true);
    ASSERT_NE(nullptr, f_flag);
    EXPECT_EQ(76, extract_precision(f_flag->get_return_type()->get_name()));
}

} // namespace