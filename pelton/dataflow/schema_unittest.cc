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

inline SchemaRef MakeSchema() {
  // Create a schema.
  std::vector<std::string> names = {"Col1", "Col2", "Col3"};
  std::vector<CType> types = {CType::UINT, CType::TEXT, CType::INT};
  std::vector<ColumnID> keys = {0};
  return SchemaFactory::Create(names, types, keys);
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

// Size is a ptr + virtual table.
TEST(SchemaTest, SchemaSize) { EXPECT_EQ(sizeof(SchemaRef), 8); }

// Test construction of SchemaRef.
TEST(SchemaTest, ConstructSchema) {
  SchemaRef schema = MakeSchema();
  CheckSchema(schema);
}
TEST(SchemaTest, ConstructSchemaFromCreateTable) {
  using C = sqlast::ColumnConstraint::Type;
  // Make a CreateTable statement.
  sqlast::CreateTable table("table1");
  sqlast::ColumnConstraint pk = sqlast::ColumnConstraint::Make(C::PRIMARY_KEY);
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
  SchemaRef schema = SchemaFactory::Create(table);
  CheckSchema(schema);
}

// SchemaRefs for the same data are equal.
TEST(SchemaTest, SchemaEquality) {
  SchemaRef schema1 = MakeSchema();
  SchemaRef schema2 = MakeSchema();
  EXPECT_EQ(schema1, schema2);
}

// SchemaRefs for the same data re-use pointer.
TEST(SchemaTest, PointerEquality) {
  SchemaRef schema1 = MakeSchema();
  SchemaRef schema2 = MakeSchema();
  EXPECT_EQ(&schema1.column_types(), &schema2.column_types());
}

}  // namespace dataflow
}  // namespace pelton
