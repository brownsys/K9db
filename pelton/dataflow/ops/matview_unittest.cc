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

// Find whether an element is in a vector or iterable.
template <typename T>
inline bool Contains(GenericIterable<T> &&it, const T &v) {
  auto end = it.end();
  return std::find(it.begin(), end, v) != end;
}
template <typename T>
inline bool Contains(const std::vector<T> &vec, const T &v) {
  return std::find(vec.cbegin(), vec.cend(), v) != vec.cend();
}

// Expects that an iterable and vector have the same elements in the same order.
template <typename T>
inline void EXPECT_EQ_ORDER(GenericIterable<T> &&l, const std::vector<T> &r) {
  size_t i = 0;
  for (const T &v : l) {
    EXPECT_EQ(v, r.at(i++));
  }
  EXPECT_EQ(i, r.size());
}

// Expects that an iterable and vector are equal (as multi-sets).
template <typename T>
inline void EXPECT_EQ_MSET(GenericIterable<T> &&l, const std::vector<T> &r) {
  std::vector<T> tmp;
  for (const T &v : r) {
    if constexpr (std::is_copy_constructible<T>::value) {
      tmp.push_back(v);
    } else {
      tmp.push_back(v.Copy());
    }
  }

  for (const T &v : l) {
    auto it = std::find(tmp.begin(), tmp.end(), v);
    // v must be found in tmp.
    EXPECT_NE(it, tmp.end());
    // Erase v from tmp, ensures that if an equal record is encountered in the
    // future, it will match a different record in r (multiset equality).
    tmp.erase(it);
  }
  EXPECT_TRUE(tmp.empty());
}

// Expects that the size of an iterable is equal to the given size.
template <typename T>
inline void EXPECT_SIZE_EQ(GenericIterable<T> &&l, size_t size) {
  size_t i = 0;
  for (const T &v : l) {
    i++;
  }
  EXPECT_EQ(i, size);
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
    EXPECT_TRUE(matview->Lookup(Key(0)).IsEmpty());
    EXPECT_TRUE(matview->Keys().IsEmpty());
  }
}

// Single entry matview.
TEST(MatViewOperatorTest, SingleMatView) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{2}};
  SchemaOwner schema{names, types, keys};

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, 0UL,
                       std::make_unique<std::string>("hello!"), -5L);

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator(keys));
  views.emplace_back(new KeyOrderedMatViewOperator(keys));
  views.emplace_back(new RecordOrderedMatViewOperator(keys, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    // Process and check.
    EXPECT_TRUE(matview->ProcessAndForward(UNDEFINED_NODE_INDEX, records));
    EXPECT_EQ(matview->count(), 1);

    // Get record by key.
    Key key = records.at(0).GetKey();
    EXPECT_TRUE(matview->Contains(key));
    EXPECT_EQ_ORDER(matview->Lookup(key), records);
    EXPECT_EQ_ORDER(matview->Keys(), {key});
  }
}

TEST(MatViewOperatorTest, SingleMatViewDifferentKey) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {1};
  Record::Compare compare{{2}};
  SchemaOwner schema{names, types, keys};

  // Create some records.
  std::vector<Record> records;
  records.emplace_back(SchemaRef(schema), true, 0UL,
                       std::make_unique<std::string>("hello!"), -5L);

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator(keys));
  views.emplace_back(new KeyOrderedMatViewOperator(keys));
  views.emplace_back(new RecordOrderedMatViewOperator(keys, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    // Process and check.
    EXPECT_TRUE(matview->ProcessAndForward(UNDEFINED_NODE_INDEX, records));
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
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{2}};
  SchemaOwner schema{names, types, keys};
  SchemaRef ref{schema};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  Record r1{ref, true, 0UL, std::move(s1), 10L};
  Record r2{ref, true, 1UL, std::move(s2), 20L};
  Record r3{ref, true, 1UL, std::move(s3), 30L};

  std::vector<Record> records;
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());

  std::vector<Record> records1;
  records1.push_back(r1.Copy());

  std::vector<Record> records2;
  records2.push_back(r2.Copy());
  records2.push_back(r3.Copy());

  // Create keys.
  std::vector<Key> all_keys = {r1.GetKey(), r2.GetKey()};

  // Create materialized views.
  std::vector<std::unique_ptr<MatViewOperator>> views;
  views.emplace_back(new UnorderedMatViewOperator(keys));
  views.emplace_back(new KeyOrderedMatViewOperator(keys));
  views.emplace_back(new RecordOrderedMatViewOperator(keys, compare));

  // Test all views.
  for (std::unique_ptr<MatViewOperator> &matview : views) {
    // Process and check.
    EXPECT_TRUE(matview->ProcessAndForward(UNDEFINED_NODE_INDEX, records));

    EXPECT_EQ(matview->count(), records.size());
    EXPECT_TRUE(matview->Contains(r1.GetKey()));
    EXPECT_TRUE(matview->Contains(r2.GetKey()));
    // Get records by key.
    EXPECT_EQ_MSET(matview->Lookup(r1.GetKey()), records1);
    EXPECT_EQ_MSET(matview->Lookup(r2.GetKey()), records2);
    // Get all keys.
    EXPECT_EQ_MSET(matview->Keys(), all_keys);
  }
}

TEST(MatViewOperatorTest, OrderedKeyTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {1};
  SchemaOwner schema{names, types, keys};
  SchemaRef ref{schema};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  Record r1{ref, true, 0UL, std::move(s1), 10L};
  Record r2{ref, true, 1UL, std::move(s2), 20L};
  Record r3{ref, true, 1UL, std::move(s3), 30L};

  std::vector<Record> records;
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());

  std::vector<Record> records1;
  std::vector<Record> records2;
  std::vector<Record> records3;
  records1.push_back(r1.Copy());
  records2.push_back(r2.Copy());
  records3.push_back(r3.Copy());

  // Create keys.
  std::vector<Key> all_keys = {r2.GetKey(), r1.GetKey(), r3.GetKey()};

  // Create materialized view.
  KeyOrderedMatViewOperator matview{keys};

  // Process and check.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));

  EXPECT_EQ(matview.count(), records.size());
  EXPECT_TRUE(matview.Contains(r1.GetKey()));
  EXPECT_TRUE(matview.Contains(r2.GetKey()));
  EXPECT_TRUE(matview.Contains(r3.GetKey()));
  // Get records by key.
  EXPECT_EQ_MSET(matview.Lookup(r1.GetKey()), records1);
  EXPECT_EQ_MSET(matview.Lookup(r2.GetKey()), records2);
  EXPECT_EQ_MSET(matview.Lookup(r3.GetKey()), records3);
  // Get all keys.
  EXPECT_EQ_ORDER(matview.Keys(), all_keys);
}

TEST(MatViewOperatorTest, OrderedRecordTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{2, 1}};
  SchemaOwner schema{names, types, keys};
  SchemaRef ref{schema};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("ABC!");

  // Create some records.
  Record r1{ref, true, 0UL, std::move(s1), 10L};
  Record r2{ref, true, 1UL, std::move(s2), 20L};
  Record r3{ref, true, 1UL, std::move(s3), 30L};
  Record r4{ref, true, 1UL, std::move(s4), 30L};

  std::vector<Record> records;
  records.push_back(r4.Copy());
  records.push_back(r1.Copy());
  records.push_back(r2.Copy());
  records.push_back(r3.Copy());
  records.push_back(r4.Copy());

  std::vector<Record> records1;
  records1.push_back(r1.Copy());

  std::vector<Record> records2;
  records2.push_back(r2.Copy());
  records2.push_back(r4.Copy());
  records2.push_back(r4.Copy());
  records2.push_back(r3.Copy());

  // Create keys.
  std::vector<Key> all_keys = {r1.GetKey(), r2.GetKey()};

  // Create materialized view.
  RecordOrderedMatViewOperator matview{keys, compare};

  // Process and check.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));

  EXPECT_EQ(matview.count(), records.size());
  EXPECT_TRUE(matview.Contains(all_keys.at(0)));
  EXPECT_TRUE(matview.Contains(all_keys.at(1)));
  // Get records by key.
  EXPECT_EQ_ORDER(matview.Lookup(all_keys.at(0)), records1);
  EXPECT_EQ_ORDER(matview.Lookup(all_keys.at(1)), records2);
  // Get all keys.
  EXPECT_EQ_MSET(matview.Keys(), all_keys);
}

TEST(MatViewOperatorTest, EmptyKeyTest) {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  Record::Compare compare{{1}};
  SchemaOwner schema{names, types, keys};
  SchemaRef ref{schema};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  Record r1{ref, true, 0UL, std::move(s1), 10L};
  Record r2{ref, true, 1UL, std::move(s2), 20L};
  Record r3{ref, true, 1UL, std::move(s3), 30L};

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
    // Process and check.
    EXPECT_TRUE(matview->ProcessAndForward(UNDEFINED_NODE_INDEX, records));

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
  SchemaOwner schema{names, types, keys};
  SchemaRef ref{schema};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Hellobye!");

  // Create some records.
  Record r1{ref, true, 0UL, std::move(s1), 10L};
  Record r2{ref, true, 1UL, std::move(s2), 20L};
  Record r3{ref, true, 1UL, std::move(s3), 30L};

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
    // Process and check.
    EXPECT_TRUE(matview->ProcessAndForward(UNDEFINED_NODE_INDEX, records));

    EXPECT_EQ(matview->count(), records.size());
    EXPECT_TRUE(matview->Contains(Key(0)));
    // Get all keys.
    EXPECT_EQ_MSET(matview->Keys(), all_keys);
  }

  // Test lookup with limit and offset.
  for (size_t i = 0; i < 2; i++) {
    EXPECT_SIZE_EQ(views.at(i)->Lookup(Key(0), 0), 0);
    EXPECT_SIZE_EQ(views.at(i)->Lookup(Key(0), 2), 2);
    EXPECT_SIZE_EQ(views.at(i)->Lookup(Key(0), 1, 1), 1);
    EXPECT_SIZE_EQ(views.at(i)->Lookup(Key(0), 2, 1), 2);
    EXPECT_SIZE_EQ(views.at(i)->Lookup(Key(0), 5), 3);
    EXPECT_SIZE_EQ(views.at(i)->Lookup(Key(0), 1, 4), 0);
  }
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
  SchemaOwner schema{names, types, keys};
  SchemaRef ref{schema};

  // Some string values.
  std::unique_ptr<std::string> s1 = std::make_unique<std::string>("Hello!");
  std::unique_ptr<std::string> s2 = std::make_unique<std::string>("Zoo!");
  std::unique_ptr<std::string> s3 = std::make_unique<std::string>("Bye!");
  std::unique_ptr<std::string> s4 = std::make_unique<std::string>("ABC!");

  // Create some records.
  Record r1{ref, true, 0UL, std::move(s1), 10L};
  Record r2{ref, true, 1UL, std::move(s2), 20L};
  Record r3{ref, true, 1UL, std::move(s3), 20L};
  Record r4{ref, true, 1UL, std::move(s4), 30L};

  std::vector<Record> records;
  records.push_back(r4.Copy());
  records.push_back(r1.Copy());
  records.push_back(r3.Copy());
  records.push_back(r2.Copy());
  records.push_back(r4.Copy());

  // Expected output.
  std::vector<Record> records1;
  records1.push_back(r2.Copy());
  records1.push_back(r4.Copy());
  records1.push_back(r4.Copy());

  std::vector<Record> records2;
  records2.push_back(r4.Copy());

  // Create materialized view.
  RecordOrderedMatViewOperator matview{{}, compare};

  // Process and check.
  EXPECT_TRUE(matview.ProcessAndForward(UNDEFINED_NODE_INDEX, records));

  EXPECT_EQ(matview.count(), records.size());
  EXPECT_TRUE(matview.Contains(Key(0)));

  // Get records by key greater than some given record..
  EXPECT_EQ_ORDER(matview.LookupGreater(Key(0), r3), records1);
  EXPECT_EQ_ORDER(matview.LookupGreater(Key(0), r3, 1, 1), records2);

  // Get all keys.
  EXPECT_EQ_MSET(matview.Keys(), std::vector<Key>{Key(0)});
}

}  // namespace dataflow
}  // namespace pelton
