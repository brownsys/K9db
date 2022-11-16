#include "pelton/sql/rocksdb/filter.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/sqlast/value_mapper.h"
#include "pelton/util/ints.h"

#define STR(s) std::make_unique<std::string>(s)
#define ADD_CONDITION(v, col, val) \
  v.AddValue(col, sqlast::Value::FromSQLString(val))

namespace pelton {
namespace sql {

using CType = sqlast::ColumnDefinition::Type;

dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
    {"ID", "name", "age", "email"},
    {CType::UINT, CType::TEXT, CType::INT, CType::TEXT}, {0});

dataflow::NullValue null;
dataflow::Record r1{schema, true, 0_u, STR("user1"), 20_s, STR("email1")};
dataflow::Record r2{schema, true, 1_u, STR("user2"), null, STR("email2")};
dataflow::Record r3{schema, true, 2_u, STR("user2"), 20_s, STR("email3")};
dataflow::Record r4{schema, true, 3_u, STR("user4"), 25_s, STR("user2")};
dataflow::Record r5{schema, true, 4_u, null, 0_s, STR("email5")};

// A no-op filter.
TEST(FilterTest, IdentityFilter) {
  sqlast::ValueMapper cond(schema);
  EXPECT_TRUE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_TRUE(InMemoryFilter(cond, r3));
  EXPECT_TRUE(InMemoryFilter(cond, r4));
  EXPECT_TRUE(InMemoryFilter(cond, r5));
}

// Simple filters.
TEST(FilterTest, SimpleFilters) {
  sqlast::ValueMapper cond(schema);
  ADD_CONDITION(cond, 0, "0");
  EXPECT_TRUE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 2, "20");
  EXPECT_TRUE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_TRUE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "'user2'");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_TRUE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 2, "NULL");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "NULL");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_TRUE(InMemoryFilter(cond, r5));
}

// OR filters.
TEST(FilterTest, OrFilters) {
  sqlast::ValueMapper cond(schema);
  ADD_CONDITION(cond, 0, "0");
  ADD_CONDITION(cond, 0, "2");
  ADD_CONDITION(cond, 0, "10");
  EXPECT_TRUE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_TRUE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 2, "20");
  ADD_CONDITION(cond, 2, "NULL");
  EXPECT_TRUE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_TRUE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "'user1'");
  ADD_CONDITION(cond, 1, "'user20'");
  ADD_CONDITION(cond, 1, "NULL");
  ADD_CONDITION(cond, 1, "'user4'");
  EXPECT_TRUE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_TRUE(InMemoryFilter(cond, r4));
  EXPECT_TRUE(InMemoryFilter(cond, r5));
}

// And filters.
TEST(FilterTest, AndFilters) {
  sqlast::ValueMapper cond(schema);
  ADD_CONDITION(cond, 0, "0");
  ADD_CONDITION(cond, 1, "'user2'");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "'user2'");
  ADD_CONDITION(cond, 2, "NULL");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "'user2'");
  ADD_CONDITION(cond, 2, "20");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_FALSE(InMemoryFilter(cond, r2));
  EXPECT_TRUE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));
}

// Combined filters.
TEST(FilterTest, Combined) {
  sqlast::ValueMapper cond(schema);
  ADD_CONDITION(cond, 0, "0");
  ADD_CONDITION(cond, 0, "1");
  ADD_CONDITION(cond, 1, "'user2'");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "'user2'");
  ADD_CONDITION(cond, 1, "NULL");
  ADD_CONDITION(cond, 2, "0");
  ADD_CONDITION(cond, 2, "NULL");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_TRUE(InMemoryFilter(cond, r5));

  cond = sqlast::ValueMapper(schema);
  ADD_CONDITION(cond, 1, "'user2'");
  ADD_CONDITION(cond, 1, "0");
  ADD_CONDITION(cond, 1, "100");
  ADD_CONDITION(cond, 2, "0");
  ADD_CONDITION(cond, 2, "NULL");
  ADD_CONDITION(cond, 2, "100");
  EXPECT_FALSE(InMemoryFilter(cond, r1));
  EXPECT_TRUE(InMemoryFilter(cond, r2));
  EXPECT_FALSE(InMemoryFilter(cond, r3));
  EXPECT_FALSE(InMemoryFilter(cond, r4));
  EXPECT_FALSE(InMemoryFilter(cond, r5));
}

}  // namespace sql
}  // namespace pelton
