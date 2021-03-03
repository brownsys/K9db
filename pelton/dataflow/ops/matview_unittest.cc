#include "pelton/dataflow/ops/matview.h"

#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/key.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline bool Contains(const std::vector<Record> &records, const Record &record) {
  return std::find(records.cbegin(), records.cend(), record) != records.end();
}

// Empty matview.
TEST(MatViewOperatorTest, EmptyMatView) {
  // Empty mat view.
  MatViewOperator matview{std::vector<ColumnID>{0}};
  EXPECT_EQ(std::begin(matview), std::end(matview));
  EXPECT_EQ(matview.count(), 0);
  EXPECT_EQ(matview.Lookup(Key(0UL)), std::vector<Record>{});
  EXPECT_TRUE(!matview.Contains(Key(0UL)));
}

// Single entry matview.
TEST(MatViewOperatorTest, SingleMatView) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaOwner schema{names, types, keys};

  // Create materialized view.
  MatViewOperator matview{keys};

  // Create a single record.
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema));
  records.at(0).SetUInt(0ULL, 0);
  records.at(0).SetString(std::make_unique<std::string>("hello!"), 1);
  records.at(0).SetInt(-5L, 2);

  // Process and check.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));
  EXPECT_EQ(matview.count(), 1);

  // Get record by key.
  Key key = records.at(0).GetKey();
  EXPECT_TRUE(matview.Contains(key));
  EXPECT_EQ(matview.Lookup(key), records);

  // Compare by iteration.
  auto it = std::begin(matview);
  EXPECT_EQ(records.at(0), *it);
  EXPECT_EQ(++it, std::end(matview));
}

TEST(MatViewOperatorTest, IteratorTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaOwner schema{names, types, keys};

  // Create materialized view.
  MatViewOperator matview{keys};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema));
  records.at(0).SetUInt(0ULL, 0);
  records.at(0).SetString(std::move(s1), 1);
  records.at(0).SetInt(10LL, 2);

  records.emplace_back(SchemaRef(schema));
  records.at(1).SetUInt(1ULL, 0);
  records.at(1).SetString(std::move(s2), 1);
  records.at(1).SetInt(20LL, 2);

  records.emplace_back(SchemaRef(schema));
  records.at(2).SetUInt(0ULL, 0);
  records.at(2).SetString(std::move(s3), 1);
  records.at(2).SetInt(30LL, 2);

  // Process records.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));

  // Inputs and outputs must be the same (modulo order).
  size_t count = 0;
  for (const Record &record : matview) {
    EXPECT_TRUE(Contains(records, record));
    count++;
  }
  EXPECT_EQ(records.size(), count);
  EXPECT_EQ(matview.count(), count);
}

TEST(MatViewOperatorTest, LookupTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaOwner schema{names, types, keys};

  // Create materialized view.
  MatViewOperator matview{keys};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema));
  records.at(0).SetUInt(0ULL, 0);
  records.at(0).SetString(std::move(s1), 1);
  records.at(0).SetInt(10LL, 2);

  records.emplace_back(SchemaRef(schema));
  records.at(1).SetUInt(1ULL, 0);
  records.at(1).SetString(std::move(s2), 1);
  records.at(1).SetInt(20LL, 2);

  records.emplace_back(SchemaRef(schema));
  records.at(2).SetUInt(0ULL, 0);
  records.at(2).SetString(std::move(s3), 1);
  records.at(2).SetInt(30LL, 2);

  // Process records.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));

  // Count and contains.
  EXPECT_EQ(matview.count(), records.size());
  EXPECT_TRUE(matview.Contains(records.at(0).GetKey()));
  EXPECT_TRUE(matview.Contains(records.at(1).GetKey()));
  EXPECT_TRUE(matview.Contains(records.at(2).GetKey()));

  // Lookup by key.
  const std::vector<Record> &records1 = matview.Lookup(records.at(0).GetKey());
  const std::vector<Record> &records2 = matview.Lookup(records.at(1).GetKey());
  EXPECT_EQ(records1.size(), 2);
  EXPECT_EQ(records2.size(), 1);
  EXPECT_EQ(records1.at(0), records.at(0));
  EXPECT_EQ(records1.at(1), records.at(2));
  EXPECT_EQ(records2.at(0), records.at(1));
}

TEST(MatViewOperatorTest, DifferentKeysTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  SchemaOwner schema{names, types, keys};

  // Create materialized view.
  std::vector<ColumnID> matview_keys = {1};
  MatViewOperator matview{matview_keys};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s5 = std::make_unique<std::string>("Bye!");

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema));
  records.at(0).SetUInt(0ULL, 0);
  records.at(0).SetString(std::move(s1), 1);
  records.at(0).SetInt(10LL, 2);

  records.emplace_back(SchemaRef(schema));
  records.at(1).SetUInt(1ULL, 0);
  records.at(1).SetString(std::move(s2), 1);
  records.at(1).SetInt(20LL, 2);

  records.emplace_back(SchemaRef(schema));
  records.at(2).SetUInt(0ULL, 0);
  records.at(2).SetString(std::move(s3), 1);
  records.at(2).SetInt(30LL, 2);

  // Process records.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));

  // Inputs and outputs must be the same (modulo order).
  size_t count = 0;
  for (const Record &record : matview) {
    EXPECT_TRUE(Contains(records, record));
    count++;
  }
  EXPECT_EQ(records.size(), count);
  EXPECT_EQ(matview.count(), count);

  // Remove some records.
  std::vector<Record> removed;
  removed.emplace_back(SchemaRef(schema), false, 0UL, std::move(s4), 10L);
  removed.emplace_back(SchemaRef(schema), false, 1UL, std::move(s5), 20L);

  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, removed));

  // Inputs were removed from output.
  count = 0;
  for (const Record &record : matview) {
    // EXPECT_EQ(record, records.at(2));
    count++;
  }
  EXPECT_EQ(1, count);
  EXPECT_EQ(matview.count(), count);
}

}  // namespace dataflow
}  // namespace pelton
