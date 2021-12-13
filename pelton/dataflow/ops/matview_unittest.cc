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
#include "pelton/util/ints.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

// Expects that two vectors are equal with the same order.
template <typename T>
inline void EXPECT_EQ_ORDER(std::vector<T> &&l, const std::vector<T> &r) {
  EXPECT_EQ(l.size(), r.size());
  size_t i = 0;
  for (const T &v : l) {
    EXPECT_EQ(v, r.at(i++));
  }
}

// Expects that two vectors are equal (as multi-sets).
template <typename T>
inline void EXPECT_EQ_MSET(std::vector<T> &&l, const std::vector<T> &r) {
  for (const T &v : r) {
    auto it = std::find(l.begin(), l.end(), v);
    // v must be found in tmp.
    EXPECT_NE(it, l.end());
    // Erase v from tmp, ensures that if an equal record is encountered in the
    // future, it will match a different record in r (multiset equality).
    l.erase(it);
  }
  EXPECT_TRUE(l.empty());
}

std::vector<Record> CopyVec(const std::vector<Record> &records) {
  std::vector<Record> copy;
  copy.reserve(records.size());
  for (const Record &r : records) {
    copy.push_back(r.Copy());
  }
  return copy;
}

// Empty matview.
TEST(MatViewOperatorTest, EmptyMatView) {
  Record::Compare compare{{}};

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator({0}));
  views.emplace_back(new KeyOrderedMatViewOperator({0}));
  views.emplace_back(new RecordOrderedMatViewOperator({0}, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    EXPECT_EQ(matview->count(), 0);
    EXPECT_TRUE(!matview->Contains(Key(0)));
    EXPECT_EQ(matview->Lookup(Key(0)).size(), 0);
    EXPECT_EQ(matview->Keys().size(), 0);
    EXPECT_EQ(matview->All().size(), 0);
  }
}

// Single entry matview.
TEST(MatViewOperatorTest, SingleMatView) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{2}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 1_u,
                       std::make_unique<std::string>("hello!"), -5_s);

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator(keys));
  views.emplace_back(new KeyOrderedMatViewOperator(keys));
  views.emplace_back(new RecordOrderedMatViewOperator(keys, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->input_schemas_.push_back(schema);
    matview->ComputeOutputSchema();

    // Process and check.
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                               Promise::None.Derive());
    EXPECT_EQ(matview->count(), 1);

    // Get record by key.
    Key key = records.at(0).GetKey();
    EXPECT_TRUE(matview->Contains(key));
    EXPECT_EQ_ORDER(matview->Lookup(key), records);
    EXPECT_EQ_ORDER(matview->Keys(), {key});
    EXPECT_EQ_ORDER(matview->All(), records);
  }

  // Get record by key order.
  auto key_op = static_cast<KeyOrderedMatViewOperator *>(views.at(1).get());
  Key key0(1);
  key0.AddValue(0_u);
  Key key1 = records.at(0).GetKey();
  Key key2(1);
  key2.AddValue(2_u);
  EXPECT_EQ_ORDER(key_op->KeysGreater(key0), {key1});
  EXPECT_EQ_ORDER(key_op->KeysGreater(key1), {key1});
  EXPECT_EQ_ORDER(key_op->KeysGreater(key2), {});
  EXPECT_EQ_ORDER(key_op->LookupGreater(key0), records);
  EXPECT_EQ_ORDER(key_op->LookupGreater(key1), records);
  EXPECT_EQ_ORDER(key_op->LookupGreater(key2), {});

  // Get record by record order.
  auto rec_op = static_cast<RecordOrderedMatViewOperator *>(views.at(2).get());
  Record r0{schema, true, 100_u, std::make_unique<std::string>(""), -10_s};
  Record r1{schema, true, 100_u, std::make_unique<std::string>(""), -5_s};
  Record r2{schema, true, 100_u, std::make_unique<std::string>(""), 0_s};
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, r0), records);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, r1), records);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, r2), {});

  // Add negative records.
  records.at(0).SetPositive(false);
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                               Promise::None.Derive());

    Key key = records.at(0).GetKey();
    EXPECT_FALSE(matview->Contains(key));
    EXPECT_EQ_ORDER(matview->All(), {});
    EXPECT_EQ_ORDER(matview->Lookup(key), {});
    EXPECT_EQ_ORDER(matview->Keys(), {});
  }
}

TEST(MatViewOperatorTest, SingleMatViewDifferentKey) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {1};
  Record::Compare compare{{2}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(schema, true, 0_u,
                       std::make_unique<std::string>("hello!"), -5_s);

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator(keys));
  views.emplace_back(new KeyOrderedMatViewOperator(keys));
  views.emplace_back(new RecordOrderedMatViewOperator(keys, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->input_schemas_.push_back(schema);
    matview->ComputeOutputSchema();

    // Process and check.
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                               Promise::None.Derive());
    EXPECT_EQ(matview->count(), 1);

    // Get record by key.
    Key key = records.at(0).GetValues({1});
    EXPECT_TRUE(matview->Contains(key));
    EXPECT_EQ_ORDER(matview->Lookup(key), records);
    EXPECT_EQ_ORDER(matview->Keys(), {key});
  }
}

TEST(MatViewOperatorTest, ProcessBatchTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> pk = {0};
  Record::Compare compare{{2}};
  SchemaRef schema = SchemaFactory::Create(names, types, pk);

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("Deleteme!");
  std::unique_ptr<std::string> s5 = std::make_unique<std::string>("Deleteme!");

  // Create some records.
  Record r1{schema, true, 0_u, std::move(s1), 10_s};
  Record r2{schema, true, 1_u, std::move(s2), 20_s};
  Record r3{schema, true, 2_u, std::move(s3), 30_s};
  Record r4{schema, true, 3_u, std::move(s4), 40_s};
  Record r5{schema, false, 3_u, std::move(s5), 40_s};

  std::vector<Record> records;
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());

  std::vector<Record> process;
  process.push_back(r1.Copy());
  process.push_back(r2.Copy());
  process.push_back(r3.Copy());
  process.push_back(r4.Copy());
  process.push_back(r5.Copy());

  std::vector<Record> records1;
  records1.push_back(r1.Copy());
  records1.push_back(r2.Copy());

  std::vector<Record> record1_half;
  record1_half.push_back(r2.Copy());

  std::vector<Record> records2;
  records2.push_back(r3.Copy());

  // Create materialized views.
  std::vector<ColumnID> keys = {1};
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator(keys));
  views.emplace_back(new KeyOrderedMatViewOperator(keys));
  views.emplace_back(new RecordOrderedMatViewOperator(keys, compare));

  // Create keys.
  std::vector<Key> all_keys = {r1.GetValues(keys), r3.GetValues(keys)};

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->input_schemas_.push_back(schema);
    matview->ComputeOutputSchema();

    // Process and check.
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(process),
                               Promise::None.Derive());

    EXPECT_EQ(matview->count(), records.size());
    EXPECT_TRUE(matview->Contains(r1.GetValues(keys)));
    EXPECT_TRUE(matview->Contains(r2.GetValues(keys)));
    EXPECT_TRUE(matview->Contains(r3.GetValues(keys)));
    // Get records by key.
    EXPECT_EQ_MSET(matview->Lookup(r1.GetValues(keys)), records1);
    EXPECT_EQ_MSET(matview->Lookup(r2.GetValues(keys)), records1);
    EXPECT_EQ_MSET(matview->Lookup(r3.GetValues(keys)), records2);
    // Get all keys.
    EXPECT_EQ_MSET(matview->Keys(), all_keys);
    // Get all records.
    EXPECT_EQ_MSET(matview->All(), records);
  }

  // Get record by key order.
  auto key_op = static_cast<KeyOrderedMatViewOperator *>(views.at(1).get());
  Key key0(1);
  key0.AddValue("A");
  Key key1(1);
  key1.AddValue("Hello!");
  Key key2(1);
  key2.AddValue("Helloby");
  Key key3(1);
  key3.AddValue("Hellobye!");
  Key key4(1);
  key4.AddValue("Hellobye!!");
  EXPECT_EQ_ORDER(key_op->KeysGreater(key0), {key1, key3});
  EXPECT_EQ_ORDER(key_op->KeysGreater(key1), {key1, key3});
  EXPECT_EQ_ORDER(key_op->KeysGreater(key2), {key3});
  EXPECT_EQ_ORDER(key_op->KeysGreater(key3), {key3});
  EXPECT_EQ_ORDER(key_op->KeysGreater(key4), {});
  EXPECT_EQ_ORDER(key_op->LookupGreater(key0), records);
  EXPECT_EQ_ORDER(key_op->LookupGreater(key1), records);
  EXPECT_EQ_ORDER(key_op->LookupGreater(key2), records2);
  EXPECT_EQ_ORDER(key_op->LookupGreater(key3), records2);
  EXPECT_EQ_ORDER(key_op->LookupGreater(key4), {});

  // Get record by record order.
  auto rec_op = static_cast<RecordOrderedMatViewOperator *>(views.at(2).get());
  Record rc0{schema, true, 100_u, std::make_unique<std::string>(""), 10_s};
  Record rc1{schema, true, 100_u, std::make_unique<std::string>(""), 15_s};
  Record rc2{schema, true, 100_u, std::make_unique<std::string>(""), 30_s};
  Record rc3{schema, true, 100_u, std::make_unique<std::string>(""), 35_s};
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, rc0), records1);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, rc1), record1_half);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, rc2), {});
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key1, rc3), {});
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key3, rc0), records2);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key3, rc1), records2);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key3, rc2), records2);
  EXPECT_EQ_ORDER(rec_op->LookupGreater(key3, rc3), {});
}

TEST(MatViewOperatorTest, EmptyKeyTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{1}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  Record r1{schema, true, 0_u, std::move(s1), 10_s};
  Record r2{schema, true, 1_u, std::move(s2), 20_s};
  Record r3{schema, true, 2_u, std::move(s3), 30_s};

  std::vector<Record> records;
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());

  std::vector<Record> ordered;
  ordered.push_back(r2.Copy());
  ordered.push_back(r1.Copy());
  ordered.push_back(r3.Copy());

  // Create keys.
  std::vector<Key> all_keys = {Key(0)};

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator({}));
  views.emplace_back(new KeyOrderedMatViewOperator({}));
  views.emplace_back(new RecordOrderedMatViewOperator({}, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->input_schemas_.push_back(schema);
    matview->ComputeOutputSchema();

    // Process and check.
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                               Promise::None.Derive());

    EXPECT_EQ(matview->count(), records.size());
    EXPECT_TRUE(matview->Contains(Key(0)));
    // Get records by key.
    EXPECT_EQ_MSET(matview->Lookup(Key(0)), records);
    // Get all keys.
    EXPECT_EQ_MSET(matview->Keys(), all_keys);
  }
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0)), ordered);
}

TEST(MatViewOperatorTest, LimitTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{1}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  Record r1{schema, true, 0_u, std::move(s1), 10_s};
  Record r2{schema, true, 1_u, std::move(s2), 20_s};
  Record r3{schema, true, 2_u, std::move(s3), 30_s};

  std::vector<Record> records;
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());

  // The expected result of Looking up with various limits and offsets.
  std::vector<Record> empty;

  std::vector<Record> ordered0;
  ordered0.push_back(r2.Copy());
  ordered0.push_back(r1.Copy());

  std::vector<Record> ordered1;
  ordered1.push_back(r1.Copy());

  std::vector<Record> ordered2;
  ordered2.push_back(r1.Copy());
  ordered2.push_back(r3.Copy());

  std::vector<Record> ordered3;
  ordered3.push_back(r2.Copy());
  ordered3.push_back(r1.Copy());
  ordered3.push_back(r3.Copy());

  // Create keys.
  std::vector<Key> all_keys = {Key(0)};

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator({}));
  views.emplace_back(new KeyOrderedMatViewOperator({}));
  views.emplace_back(new RecordOrderedMatViewOperator({}, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->input_schemas_.push_back(schema);
    matview->ComputeOutputSchema();

    // Process and check.
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                               Promise::None.Derive());

    EXPECT_EQ(matview->count(), records.size());
    EXPECT_TRUE(matview->Contains(Key(0)));
    // Get all keys.
    EXPECT_EQ_MSET(matview->Keys(), all_keys);
    // Test lookup with limit and offset.
    EXPECT_EQ(matview->Lookup(Key(0), 0).size(), 0);
    EXPECT_EQ(matview->Lookup(Key(0), 2).size(), 2);
    EXPECT_EQ(matview->Lookup(Key(0), 1, 1).size(), 1);
    EXPECT_EQ(matview->Lookup(Key(0), 2, 1).size(), 2);
    EXPECT_EQ(matview->Lookup(Key(0), 5).size(), 3);
    EXPECT_EQ(matview->Lookup(Key(0), 1, 4).size(), 0);
  }

  // Test lookup with limit and offset.
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0), 0), empty);
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0), 2), ordered0);
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0), 1, 1), ordered1);
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0), 2, 1), ordered2);
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0), 5), ordered3);
  EXPECT_EQ_ORDER(views.at(2)->Lookup(Key(0), 1, 4), empty);
}

TEST(MatViewOperatorTest, LookupGreater) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{2, 1}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Zoo!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("ABC!");

  // Create some records.
  Record r1{schema, true, 0_u, std::move(s1), 10_s};
  Record r2{schema, true, 2_u, std::move(s2), 20_s};
  Record r3{schema, true, 1_u, std::move(s3), 20_s};
  Record r4{schema, true, 5_u, std::move(s4), 30_s};
  Record r10{schema, true, 15_u, std::make_unique<std::string>(""), 100_s};

  std::vector<Record> records;
  records.push_back(r4.Copy());
  records.push_back(r1.Copy());
  records.push_back(r3.Copy());
  records.push_back(r2.Copy());

  // Expected output.
  std::vector<Record> records1;
  records1.push_back(r3.Copy());
  records1.push_back(r2.Copy());
  records1.push_back(r4.Copy());

  std::vector<Record> records2;
  records2.push_back(r2.Copy());

  std::vector<Record> sorted;
  sorted.push_back(r1.Copy());
  sorted.push_back(r3.Copy());
  sorted.push_back(r2.Copy());
  sorted.push_back(r4.Copy());

  // Create materialized view.
  RecordOrderedMatViewOperator matview{{}, compare};
  static_cast<MatViewOperator *>(&matview)->input_schemas_.push_back(schema);

  // Process and check.
  matview.ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                            Promise::None.Derive());

  EXPECT_EQ(matview.count(), records.size());
  EXPECT_EQ_MSET(matview.All(), records);
  EXPECT_EQ_ORDER(matview.All(), sorted);
  EXPECT_TRUE(matview.Contains(Key(0)));

  // Get records by key greater than some given record..
  EXPECT_EQ_ORDER(matview.LookupGreater(Key(0), r3), records1);
  EXPECT_EQ_ORDER(matview.LookupGreater(Key(0), r3, 1, 1), records2);
  EXPECT_EQ_ORDER(matview.LookupGreater(Key(0), r10), {});

  // Get all keys.
  EXPECT_EQ_MSET(matview.Keys(), std::vector<Key>{Key(0)});
}

TEST(MatViewOperatorTest, AllOnRecordOrdered) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{1, 2}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("ABC!");
  std::unique_ptr<std::string> s5 = std::make_unique<std::string>("");

  // Create some records.
  Record r1{schema, true, 0_u, std::move(s1), 10_s};
  Record r2{schema, true, 1_u, std::move(s2), 20_s};
  Record r3{schema, true, 2_u, std::move(s3), 20_s};
  Record r4{schema, true, 3_u, std::move(s4), 30_s};
  Record r5{schema, true, 4_u, std::move(s5), 100_s};

  std::vector<Record> records;
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());
  records.push_back(r4.Copy());
  records.push_back(r5.Copy());

  // Expected output.
  std::vector<Record> sorted;
  sorted.push_back(r5.Copy());
  sorted.push_back(r4.Copy());
  sorted.push_back(r3.Copy());
  sorted.push_back(r1.Copy());
  sorted.push_back(r2.Copy());

  // Create materialized view.
  RecordOrderedMatViewOperator matview{keys, compare};
  static_cast<MatViewOperator *>(&matview)->input_schemas_.push_back(schema);

  // Process and check.
  matview.ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(records),
                            Promise::None.Derive());
  EXPECT_EQ(matview.count(), records.size());
  EXPECT_EQ_MSET(matview.All(), records);
  EXPECT_EQ_ORDER(matview.All(), sorted);
}

TEST(MatViewOperatorTest, NightmareScenarioNegativePositiveOutOfOrder) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};  // Has PK, will use PK index.
  Record::Compare compare{{2}};
  SchemaRef schema = SchemaFactory::Create(names, types, keys);

  // Matview. Key is Col2.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator({1}));
  views.emplace_back(new KeyOrderedMatViewOperator({1}));
  views.emplace_back(new RecordOrderedMatViewOperator({1}, compare));

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s5 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s6 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> es1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> es2 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> es3 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> es4 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> es5 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> es6 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> es7 = std::make_unique<std::string>("Hello!");

  // Some keys for lookup.
  Key khello(1);
  Key kbye(1);
  khello.AddValue("Hello!");
  kbye.AddValue("Bye!");

  // Create some records.
  std::vector<Record> first_batch;
  first_batch.emplace_back(schema, true, 0_u, std::move(s1), 10_s);
  first_batch.emplace_back(schema, true, 1_u, std::move(s2), 10_s);
  first_batch.emplace_back(schema, true, 2_u, std::move(s3), 20_s);
  // First and second are in order.
  std::vector<Record> second_batch;
  second_batch.emplace_back(schema, false, 1_u, std::move(s4), 10_s);
  // Third and fourth are out of order.
  std::vector<Record> third_batch;
  third_batch.emplace_back(schema, true, 0_u, std::move(s5), 30_s);
  std::vector<Record> fourth_batch;
  fourth_batch.emplace_back(schema, false, 0_u, std::move(s6), 10_s);

  // Expected records.
  std::vector<Record> ebye;
  ebye.emplace_back(schema, true, 2_u, std::move(es3), 20_s);
  std::vector<Record> e1;
  e1.emplace_back(schema, true, 0_u, std::move(es1), 10_s);
  e1.emplace_back(schema, true, 1_u, std::move(es2), 10_s);
  std::vector<Record> e2;
  e2.emplace_back(schema, false, 0_u, std::move(es4), 10_s);
  std::vector<Record> e3;
  e3.emplace_back(schema, true, 0_u, std::move(es5), 10_s);
  e3.emplace_back(schema, true, 0_u, std::move(es6), 30_s);
  std::vector<Record> e4;
  e4.emplace_back(schema, false, 0_u, std::move(es7), 30_s);

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    matview->input_schemas_.push_back(schema);
    matview->ComputeOutputSchema();

    // Process a batch and check.
    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(first_batch),
                               Promise::None.Derive());
    EXPECT_EQ_ORDER(matview->Lookup(khello), e1);
    EXPECT_EQ_ORDER(matview->Lookup(kbye), ebye);

    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(second_batch),
                               Promise::None.Derive());
    EXPECT_EQ_ORDER(matview->Lookup(khello), e2);
    EXPECT_EQ_ORDER(matview->Lookup(kbye), ebye);

    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(third_batch),
                               Promise::None.Derive());
    EXPECT_EQ_ORDER(matview->Lookup(khello), e3);
    EXPECT_EQ_ORDER(matview->Lookup(kbye), ebye);

    matview->ProcessAndForward(UNDEFINED_NODE_INDEX, CopyVec(fourth_batch),
                               Promise::None.Derive());
    EXPECT_EQ_ORDER(matview->Lookup(khello), e4);
    EXPECT_EQ_ORDER(matview->Lookup(kbye), ebye);
  }
}

}  // namespace dataflow
}  // namespace pelton
