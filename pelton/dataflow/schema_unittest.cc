#include "pelton/dataflow/schema.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/types.h"
#include "pelton/sqlast/ast.h"

namespace pelton {
namespace dataflow {

using CType = sqlast::ColumnDefinition::Type;

inline SchemaOwner MakeSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaOwner{names, types, keys};
}

inline void CheckSchema(const SchemaOwner &schema) {
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  EXPECT_EQ(schema.size(), 3);
  EXPECT_EQ(schema.column_names(), names);
  EXPECT_EQ(schema.column_types(), types);
  EXPECT_EQ(schema.keys(), keys);
  EXPECT_EQ(schema.NameOf(0), names.at(0));
  EXPECT_EQ(schema.NameOf(1), names.at(1));
  EXPECT_EQ(schema.NameOf(2), names.at(2));
  EXPECT_EQ(schema.TypeOf(0), types.at(0));
  EXPECT_EQ(schema.TypeOf(1), types.at(1));
  EXPECT_EQ(schema.TypeOf(2), types.at(2));
#ifndef PELTON_VALGRIND_MODE
  ASSERT_DEATH({ schema.NameOf(3); }, "index out of bounds");
  ASSERT_DEATH({ schema.TypeOf(3); }, "index out of bounds");
#endif
}

inline void CheckSchema(const SchemaRef &schema) {
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  EXPECT_EQ(schema.size(), 3);
  EXPECT_EQ(schema.column_names(), names);
  EXPECT_EQ(schema.column_types(), types);
  EXPECT_EQ(schema.keys(), keys);
  EXPECT_EQ(schema.NameOf(0), names.at(0));
  EXPECT_EQ(schema.NameOf(1), names.at(1));
  EXPECT_EQ(schema.NameOf(2), names.at(2));
  EXPECT_EQ(schema.TypeOf(0), types.at(0));
  EXPECT_EQ(schema.TypeOf(1), types.at(1));
  EXPECT_EQ(schema.TypeOf(2), types.at(2));
#ifndef PELTON_VALGRIND_MODE
  ASSERT_DEATH({ schema.NameOf(3); }, "index out of bounds");
  ASSERT_DEATH({ schema.TypeOf(3); }, "index out of bounds");
#endif
}

// Test construction of SchemaOwner.
TEST(SchemaTest, ConstructOwner) {
  SchemaOwner schema = MakeSchema();
  CheckSchema(schema);
}
TEST(SchemaTest, ConstructOwnerFromCreateTable) {
  // Make a CreateTable statement.
  sqlast::CreateTable table("table1");
  sqlast::ColumnConstraint pk(sqlast::ColumnConstraint::Type::PRIMARY_KEY);
  sqlast::ColumnDefinition col1("Col1", CType::UINT);
  col1.AddConstraint(pk);
  table.AddColumn("Col1", col1);
  table.AddColumn("Col2", sqlast::ColumnDefinition("Col2", CType::TEXT));
  table.AddColumn("Col3", sqlast::ColumnDefinition("Col3", CType::INT));
  // What we expect the schema to be.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  // Compare reality to expecation.
  SchemaOwner schema{table};
  CheckSchema(schema);
}

// Moving SchemaOwner moves data correctly and invalidates source.
TEST(SchemaTest, MoveOwner) {
  // Make a schema.
  SchemaOwner original_schema = MakeSchema();
  CheckSchema(original_schema);
  // Move schema.
  SchemaOwner moved_schema = std::move(original_schema);
  // Moved schema checks out.
  CheckSchema(moved_schema);
#ifndef PELTON_VALGRIND_MODE
  // Old schema is invalid.
  ASSERT_DEATH({ original_schema.column_names().size(); }, "");
#endif
}

// No two SchemaOwners are equal!
TEST(SchemaTest, OwnerEquality) {
  SchemaOwner owner1 = MakeSchema();
  SchemaOwner owner2 = MakeSchema();
  EXPECT_NE(owner1, owner2);
}

// SchemaRef is constructed correctly from an Owner, and does not invalidate
// the owner.
TEST(SchemaTest, ConstructRef) {
  SchemaOwner schema_owner = MakeSchema();
  SchemaRef schema_ref{schema_owner};
  // All data in schema_owner and schema_ref is what you would expect.
  CheckSchema(schema_owner);
  CheckSchema(schema_ref);
  // Same address of data.
  EXPECT_EQ(&schema_owner.column_names(), &schema_ref.column_names());
  EXPECT_EQ(&schema_owner.column_types(), &schema_ref.column_types());
  EXPECT_EQ(&schema_owner.keys(), &schema_ref.keys());
  // Owner is still intact after ref is deleted.
  schema_ref.~SchemaRef();
  CheckSchema(schema_owner);
#ifndef PELTON_VALGRIND_MODE
  // SchemaRef is invalidated.
  ASSERT_DEATH({ schema_ref.column_names().size(); }, "");
#endif
}

// Ref move and copy.
TEST(SchemaTest, MoveCopyRef) {
  // Make a schema.
  SchemaOwner schema_owner = MakeSchema();
  SchemaRef original_ref{schema_owner};
  SchemaRef copy_ref = original_ref;
  // All Schemas are fine.
  CheckSchema(schema_owner);
  CheckSchema(original_ref);
  CheckSchema(copy_ref);
  // Move SchemaRef.
  SchemaRef move_ref = std::move(copy_ref);
  CheckSchema(schema_owner);
  CheckSchema(original_ref);
  CheckSchema(move_ref);
#ifndef PELTON_VALGRIND_MODE
  // Moved ref is invalidated.
  ASSERT_DEATH({ copy_ref.column_names().size(); }, "");
#endif
}

// Refs from the same owner are equal.
TEST(SchemaTest, RefEquality) {
  // Make a schema and some copies.
  SchemaOwner schema_owner = MakeSchema();
  SchemaRef ref1{schema_owner};
  SchemaRef ref2 = ref1;
  SchemaRef ref3{schema_owner};
  SchemaRef ref4 = std::move(ref3);
  // Check equality.
  ASSERT_EQ(ref1, ref2);
  ASSERT_EQ(ref1, ref4);
  ASSERT_EQ(ref2, ref4);
  ASSERT_NE(ref1, ref3);
  ASSERT_NE(ref2, ref3);
  ASSERT_NE(ref4, ref3);
  // Check equality against owner.
  ASSERT_EQ(schema_owner, ref1);
  ASSERT_EQ(schema_owner, ref2);
  ASSERT_EQ(schema_owner, ref4);
  ASSERT_EQ(ref1, schema_owner);
  ASSERT_EQ(ref2, schema_owner);
  ASSERT_EQ(ref4, schema_owner);
  ASSERT_NE(schema_owner, ref3);
  ASSERT_NE(ref3, schema_owner);
  // Not equal to other owners.
  SchemaOwner owner2 = MakeSchema();
  ASSERT_NE(owner2, ref1);
  ASSERT_NE(owner2, ref2);
  ASSERT_NE(owner2, ref4);
  ASSERT_NE(ref1, owner2);
  ASSERT_NE(ref2, owner2);
  ASSERT_NE(ref4, owner2);
  ASSERT_NE(owner2, ref3);
  ASSERT_NE(ref3, owner2);
  // Equal when virtual.
  SchemaOwner *ptr1 = &ref1;
  ASSERT_EQ(*ptr1, schema_owner);
  ASSERT_EQ(*ptr1, ref1);
  ASSERT_EQ(*ptr1, ref2);
  ASSERT_EQ(*ptr1, ref4);
  ASSERT_EQ(schema_owner, *ptr1);
  ASSERT_EQ(ref1, *ptr1);
  ASSERT_EQ(ref2, *ptr1);
  ASSERT_EQ(ref4, *ptr1);
  ASSERT_NE(*ptr1, owner2);
  ASSERT_NE(*ptr1, ref3);
  ASSERT_NE(owner2, *ptr1);
  ASSERT_NE(ref3, *ptr1);
}

}  // namespace dataflow
}  // namespace pelton
