package com.brownsys.pelton;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.brownsys.pelton.schema.PeltonTable;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;

public class QueryPlannerTest {
  private static final QueryPlanner PLANNER;
  private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
  private static final String[] TABLES =
      new String[] {
        "CREATE TABLE tbl1 (id int, name int, age int, useless int);",
        "CREATE TABLE tbl2 (oid int, iid int, sal int);",
        "CREATE TABLE comments ("
            + "id int NOT NULL PRIMARY KEY,"
            + "created_at datetime NOT NULL,"
            + "updated_at datetime,"
            + "short_id varchar(10) NOT NULL,"
            + "story_id int NOT NULL,"
            + "user_id int NOT NULL,"
            + "parent_comment_id int,"
            + "thread_id int,"
            + "comment text NOT NULL,"
            + "upvotes int NOT NULL,"
            + "downvotes int NOT NULL,"
            + "confidence int NOT NULL,"
            + "markeddown_comment text,"
            + "is_deleted int,"
            + "is_moderated int,"
            + "is_from_email int,"
            + "hat_id int"
            + ");",
        "CREATE TABLE stories ("
            + "id int NOT NULL PRIMARY KEY,"
            + "created_at datetime,"
            + "user_id int,"
            + "url varchar(250),"
            + "title varchar(150) NOT NULL,"
            + "description text,"
            + "short_id varchar(6) NOT NULL,"
            + "is_expired int NOT NULL,"
            + "upvotes int NOT NULL,"
            + "downvotes int NOT NULL,"
            + "is_moderated int NOT NULL,"
            + "hotness int NOT NULL,"
            + "markeddown_description text,"
            + "story_cache text,"
            + "comments_count int NOT NULL,"
            + "merged_story_id int,"
            + "unavailable_at datetime,"
            + "twitter_id varchar(20),"
            + "user_is_author int"
            + ");",
        "CREATE TABLE read_ribbons ("
            + "id int NOT NULL PRIMARY KEY,"
            + "is_following int,"
            + "created_at datetime NOT NULL,"
            + "updated_at datetime NOT NULL,"
            + "user_id int,"
            + "story_id int"
            + ");"
      };

  // Helpers for initialization.
  static {
    CalciteSchema schema = CalciteSchema.createRootSchema(false, false);
    for (String sql : TABLES) {
      PeltonTable table = fromSQL(sql);
      schema.add(table.getTableName(), table);
    }
    PLANNER = new QueryPlanner(schema);
  }

  private static PeltonTable fromSQL(String sql) {
    // Get rid of excess white space.
    sql = sql.replaceAll("[ \t\n]+", " ").trim();

    // Parse sql (quickly)
    // [ ]?([A-Za-z_0-9]+) ([A-Za-z]+)(?:[ ]?\\(\\d+\\))?[^,]*[,]?[ ]?)+
    Pattern pattern = Pattern.compile("^CREATE TABLE ([^ ]+) \\((.*)\\);$");
    Matcher matcher = pattern.matcher(sql);
    if (!matcher.matches() || matcher.groupCount() < 2) {
      throw new IllegalArgumentException("Bad SQL " + sql);
    }

    // Fill in the schema information.
    PeltonTable table = new PeltonTable(matcher.group(1));
    String[] cols = matcher.group(2).split(",");
    for (String col : cols) {
      Pattern colPattern = Pattern.compile("^([A-Za-z_0-9]+) ([A-Za-z]+)[^,]*$");
      Matcher colMatcher = colPattern.matcher(col.trim());
      if (!colMatcher.matches() || colMatcher.groupCount() < 2) {
        throw new IllegalArgumentException("Bad column SQL '" + col + "' :::: " + sql);
      }
      String colName = colMatcher.group(1);
      String colTypeString = colMatcher.group(2);
      SqlTypeName colType;
      if (colTypeString.equalsIgnoreCase("int")) {
        colType = SqlTypeName.INTEGER;
      } else if (colTypeString.equalsIgnoreCase("CHAR")
          || colTypeString.equalsIgnoreCase("VARCHAR")
          || colTypeString.equalsIgnoreCase("TEXT")
          || colTypeString.equalsIgnoreCase("STRING")) {
        colType = SqlTypeName.VARCHAR;
      } else if (colTypeString.equalsIgnoreCase("DATE")
          || colTypeString.equalsIgnoreCase("DATETIME")) {
        colType = SqlTypeName.TIMESTAMP;
      } else {
        throw new IllegalArgumentException("Unrecognized column type " + colTypeString);
      }
      table.AddColumn(colName, colType);
    }

    return table;
  }

  // Helpers for testing.
  private void planAndValidate(String query, String shape) {
    LOGGER.log(Level.INFO, "Testing query: " + query);

    try {
      RelNode plan = PLANNER.getPhysicalPlan(query);
      assertTrue(
          "Optimized plan does not satisfy filter and project order",
          validateStructure(plan, false, false));
      assertTrue(
          "Optimized plan does not follow expected structure", Shape.parse(shape).matches(plan));
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Planning failed with exception", e);
      fail("planning failed with exception " + e.getMessage());
    }
  }

  // Validate that the flow is made out of compositions of (S|U)?<-P?<-F?<-[(A|J)<-P?<-F?]*<-I.
  private boolean validateStructure(RelNode plan, boolean sawProject, boolean sawFilter) {
    if (plan instanceof LogicalFilter) {
      if (sawFilter) {
        return false;
      }
      return validateStructure(plan.getInput(0), sawProject, true);
    } else if (plan instanceof LogicalProject) {
      if (sawFilter || sawProject) {
        return false;
      }
      return validateStructure(plan.getInput(0), true, false);
    } else if (plan instanceof LogicalJoin) {
      return validateStructure(plan.getInput(0), false, false)
          && validateStructure(plan.getInput(1), false, false);
    } else if (plan instanceof LogicalAggregate) {
      return validateStructure(plan.getInput(0), false, false);
    } else if (plan instanceof TableScan) {
      return true;
    } else if (plan instanceof LogicalSort) {
      if (sawProject || sawFilter) {
        return false;
      }
      return validateStructure(plan.getInput(0), false, false);
    } else if (plan instanceof LogicalUnion) {
      if (sawProject || sawFilter) {
        return false;
      }
      return validateStructure(plan.getInput(0), sawProject, true);
    } else {
      fail("Plan has unrecognized RelNode");
      return false;
    }
  }

  // Class for representing high level flow shapes. Constitutes a tree.
  private static class Shape {
    private static enum Type {
      I,
      F,
      P,
      J,
      A,
      U,
      S
    }

    private Type type;
    private ArrayList<Shape> inputs;

    public Shape(String symbol) {
      this.type = Type.valueOf(symbol);
      this.inputs = new ArrayList<Shape>();
    }

    private boolean rootMatches(RelNode node) {
      switch (this.type) {
        case A:
          return node instanceof LogicalAggregate;
        case F:
          return node instanceof LogicalFilter;
        case I:
          return node instanceof TableScan;
        case J:
          return node instanceof LogicalJoin;
        case P:
          return node instanceof LogicalProject;
        case S:
          return node instanceof LogicalSort;
        case U:
          return node instanceof LogicalUnion;
      }
      return false;
    }

    public boolean matches(RelNode node) {
      if (!this.rootMatches(node)) {
        return false;
      }
      if (this.inputs.size() != node.getInputs().size()) {
        return false;
      }
      for (int i = 0; i < this.inputs.size(); i++) {
        if (!this.inputs.get(i).matches(node.getInput(i))) {
          return false;
        }
      }
      return true;
    }

    public static Shape parse(String desc) {
      int index = desc.indexOf("<-");
      if (index < 0) { // no inputs.
        return new Shape(desc);
      }

      // Some inputs.
      Shape root = new Shape(desc.substring(0, index));
      desc = desc.substring(index + 2);
      ArrayList<String> inputs = new ArrayList<String>();
      if (desc.startsWith("[") && desc.endsWith("]")) { // More than one input.
        int counter = 0;
        String input = "";
        for (int i = 1; i < desc.length() - 1; i++) {
          assert counter >= 0;
          if (desc.charAt(i) == '[') {
            counter++;
          } else if (desc.charAt(i) == ']') {
            counter--;
          }
          if (counter == 0 && desc.charAt(i) == ',') {
            inputs.add(input);
            input = "";
          } else {
            input += desc.charAt(i);
          }
        }
        inputs.add(input);
      } else {
        inputs.add(desc);
      }

      // Parse the inputs recursively.
      for (String input : inputs) {
        root.addInput(Shape.parse(input));
      }
      return root;
    }

    private void addInput(Shape input) {
      this.inputs.add(input);
    }

    @Override
    public String toString() {
      return this.toString("");
    }

    public String toString(String indent) {
      String str = indent + this.type;
      indent += " ";
      for (Shape input : this.inputs) {
        str += input.toString(indent);
      }
      return str;
    }
  }

  // Tests!
  // Aggregate queries.
  @Test
  public void aggFilterTest() {
    planAndValidate(
        "SELECT id AS MID, SUM(age) as MSUM FROM tbl1 WHERE name > 10 GROUP BY id", "A<-P<-F<-I");
  }

  @Test
  public void aggFilterProject() {
    planAndValidate(
        "SELECT SUM(age) as MSUM FROM tbl1 WHERE name > 10 GROUP BY id", "P<-A<-P<-F<-I");
  }

  @Test
  public void aggInFilter() {
    planAndValidate(
        "SELECT id AS MID FROM tbl1 WHERE name > 10 GROUP BY id HAVING SUM(age) > ?",
        "P<-F<-A<-P<-F<-I");
  }

  @Test
  public void aggHaving() {
    planAndValidate(
        "SELECT id AS MID, SUM(age) as MSUM FROM tbl1 GROUP BY id HAVING id > ?", "A<-P<-F<-I");
    planAndValidate(
        "SELECT id AS MID, SUM(age) as MSUM FROM tbl1 WHERE name > ? GROUP BY id HAVING id > 10",
        "A<-P<-F<-I");
  }

  // Join queries.
  @Test
  public void joinProject() {
    planAndValidate(
        "SELECT tbl1.name AS MNAME, age AS MAGE, sal AS MSAL FROM tbl1 JOIN tbl2 ON tbl1.id ="
            + " tbl2.iid",
        "P<-J<-[P<-I,P<-I]");
  }

  @Test
  public void joinFilterPushable() {
    planAndValidate(
        "SELECT * FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.iid WHERE tbl1.age > 20 AND tbl2.sal > 10",
        "P<-J<-[F<-I,F<-I]");
  }

  @Test
  public void joinFilterHalfPushable() {
    planAndValidate(
        "SELECT tbl1.id, tbl2.oid, tbl2.sal FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.oid WHERE"
            + " tbl1.age > tbl2.sal AND tbl1.age > 10",
        "P<-F<-J<-[P<-F<-I,P<-I]");
    planAndValidate(
        "SELECT tbl1.id, tbl2.oid, tbl2.sal FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.oid WHERE"
            + " tbl1.age > tbl2.sal AND tbl2.sal > 10",
        "P<-F<-J<-[P<-I,P<-F<-I]");
    planAndValidate(
        "SELECT tbl1.id, tbl2.sal FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.oid WHERE tbl1.age >"
            + " tbl2.sal AND tbl1.age > 10 AND tbl2.sal > 20",
        "P<-F<-J<-[P<-F<-I,P<-F<-I]");
  }

  @Test
  public void JoinFilterOuter() {
    planAndValidate(
        "SELECT * FROM tbl1 LEFT JOIN tbl2 ON tbl1.id = tbl2.iid WHERE tbl1.age > 20 AND tbl2.sal"
            + " > 10",
        "P<-F<-J<-[F<-I,I]");
    planAndValidate(
        "SELECT * FROM tbl1 RIGHT JOIN tbl2 ON tbl1.id = tbl2.iid WHERE tbl1.age > 20 AND tbl2.sal"
            + " > 10",
        "P<-F<-J<-[I,F<-I]");
    planAndValidate(
        "SELECT * FROM tbl1 FULL OUTER JOIN tbl2 ON tbl1.id = tbl2.iid WHERE tbl1.age > 20 AND"
            + " tbl2.sal > 10",
        "P<-F<-J<-[I,I]");
  }

  @Test
  public void JoinFilterOn() {
    planAndValidate(
        "SELECT sal AS MSAL FROM tbl1 JOIN tbl2 ON tbl1.id = tbl2.iid AND tbl1.age > 20 WHERE"
            + " tbl1.name > 10 AND tbl1.age > tbl2.sal",
        "P<-F<-J<-[P<-F<-I,P<-I]");
  }

  @Test
  public void LobstersQ36() {
    planAndValidate(
        "SELECT read_ribbons.user_id, COUNT(*) FROM read_ribbons JOIN stories ON"
            + " (read_ribbons.story_id = stories.id) JOIN comments ON (read_ribbons.story_id ="
            + " comments.story_id) LEFT JOIN comments AS parent_comments ON"
            + " (comments.parent_comment_id = parent_comments.id) WHERE read_ribbons.is_following"
            + " = 1 AND comments.user_id <> read_ribbons.user_id AND comments.is_deleted = 0  AND"
            + " comments.is_moderated = 0 AND ( comments.upvotes - comments.downvotes ) >= 0 AND"
            + " read_ribbons.updated_at < comments.created_at AND ( ( parent_comments.user_id ="
            + " read_ribbons.user_id AND ( parent_comments.upvotes - parent_comments.downvotes )"
            + " >= 0 ) OR ( parent_comments.id IS NULL AND stories.user_id = read_ribbons.user_id"
            + " ) ) GROUP BY read_ribbons.user_id",
        "A<-P<-F<-J<-[P<-F<-J<-[P<-J<-[P<-F<-I,P<-I],P<-F<-I],P<-I]");
  }
}
