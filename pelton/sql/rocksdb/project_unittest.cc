#include "pelton/sql/rocksdb/project.h"

#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pelton/dataflow/record.h"
#include "pelton/dataflow/schema.h"
#include "pelton/util/ints.h"

#define STR(s) std::make_unique<std::string>(s)

#define COL(s) sqlast::Select::ResultColumn(s)
#define SQL(s) sqlast::Select::ResultColumn(sqlast::Value::FromSQLString(s))

namespace pelton {
namespace sql {

using CType = sqlast::ColumnDefinition::Type;

dataflow::SchemaRef schema = dataflow::SchemaFactory::Create(
    {"ID", "name", "age", "email"},
    {CType::UINT, CType::TEXT, CType::INT, CType::TEXT}, {0});

dataflow::Record record{schema, true, 0_u, STR("user1"), 20_s, STR("email1")};

// A no-op project.
TEST(ProjectTest, IdentityProject) {
  Projection projection = ProjectionSchema(
      schema, {COL("ID"), COL("name"), COL("age"), COL("email")});

  dataflow::Record result = Project(projection, record);
  EXPECT_EQ(result.schema(), schema);
  EXPECT_EQ(result, record);
}

// A re-ordering project.
TEST(ProjectTest, ReorderProject) {
  Projection projection = ProjectionSchema(
      schema, {COL("name"), COL("email"), COL("age"), COL("ID")});

  dataflow::Record result = Project(projection, record);

  // Check schema.
  EXPECT_EQ(result.schema(),
            dataflow::SchemaFactory::Create(
                {"name", "email", "age", "ID"},
                {CType::TEXT, CType::TEXT, CType::INT, CType::UINT}, {3}));

  // Check record.
  EXPECT_EQ(result.GetString(0), "user1");
  EXPECT_EQ(result.GetString(1), "email1");
  EXPECT_EQ(result.GetInt(2), 20);
  EXPECT_EQ(result.GetUInt(3), 0);
}

// A dropping project.
TEST(ProjectTest, DroppingProject) {
  Projection projection =
      ProjectionSchema(schema, {COL("ID"), COL("age"), COL("email")});

  dataflow::Record result = Project(projection, record);

  // Check schema.
  EXPECT_EQ(result.schema(), dataflow::SchemaFactory::Create(
                                 {"ID", "age", "email"},
                                 {CType::UINT, CType::INT, CType::TEXT}, {0}));

  // Check record.
  EXPECT_EQ(result.GetUInt(0), 0);
  EXPECT_EQ(result.GetInt(1), 20);
  EXPECT_EQ(result.GetString(2), "email1");
}

// A literal project.
TEST(ProjectTest, LiteralProject) {
  Projection projection =
      ProjectionSchema(schema, {COL("ID"), COL("name"), COL("age"),
                                COL("email"), SQL("10"), SQL("'kinan'")});

  dataflow::Record result = Project(projection, record);

  // Check schema.
  EXPECT_EQ(result.schema(),
            dataflow::SchemaFactory::Create(
                {"ID", "name", "age", "email", "10", "'kinan'"},
                {CType::UINT, CType::TEXT, CType::INT, CType::TEXT, CType::INT,
                 CType::TEXT},
                {0}));

  // Check record.
  EXPECT_EQ(result.GetUInt(0), 0);
  EXPECT_EQ(result.GetString(1), "user1");
  EXPECT_EQ(result.GetInt(2), 20);
  EXPECT_EQ(result.GetString(3), "email1");
  EXPECT_EQ(result.GetInt(4), 10);
  EXPECT_EQ(result.GetString(5), "kinan");
}

// All combined.
TEST(ProjectTest, CombinedProject) {
  Projection projection = ProjectionSchema(
      schema,
      {COL("email"), SQL("10"), COL("name"), COL("age"), SQL("'kinan'")});

  dataflow::Record result = Project(projection, record);

  // Check schema.
  EXPECT_EQ(
      result.schema(),
      dataflow::SchemaFactory::Create(
          {"email", "10", "name", "age", "'kinan'"},
          {CType::TEXT, CType::INT, CType::TEXT, CType::INT, CType::TEXT}, {}));

  // Check record.
  EXPECT_EQ(result.GetString(0), "email1");
  EXPECT_EQ(result.GetInt(1), 10);
  EXPECT_EQ(result.GetString(2), "user1");
  EXPECT_EQ(result.GetInt(3), 20);
  EXPECT_EQ(result.GetString(4), "kinan");
}

}  // namespace sql
}  // namespace pelton
