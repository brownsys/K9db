package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import com.brownsys.pelton.operators.AggregateOperatorFactory;
import com.brownsys.pelton.operators.FilterOperatorFactory;
import com.brownsys.pelton.operators.InputOperatorFactory;
import com.brownsys.pelton.operators.JoinOperatorFactory;
import com.brownsys.pelton.operators.MatViewOperatorFactory;
import com.brownsys.pelton.operators.ProjectOperatorFactory;
import com.brownsys.pelton.operators.UnionOperatorFactory;
import java.util.ArrayList;
import java.util.Stack;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;

public class PhysicalPlanVisitor extends RelShuttleImpl {
  private final PlanningContext context;
  private final Stack<ArrayList<Integer>> childrenOperators;

  public PhysicalPlanVisitor(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.context = new PlanningContext(generator);
    this.childrenOperators = new Stack<ArrayList<Integer>>();
  }

  /*
   * Main entry point. This is called by QueryPlanner.java to transform Calcite's query
   * plan to a pelton data flow.
   */
  public void populateGraph(RelNode plan) {
    this.childrenOperators.push(new ArrayList<Integer>());

    // Start visiting.
    plan.accept(this);

    // Check if a mat view operator was already created, otherwise create one.
    ArrayList<Integer> children = this.childrenOperators.pop();
    if (children.size() > 0) {
      MatViewOperatorFactory matviewFactory = new MatViewOperatorFactory(this.context);
      matviewFactory.createOperator(children);

      assert this.childrenOperators.isEmpty();
    }
  }

  /*
   * Actual planning code.
   */

  private int analyzeTableScan(TableScan scan) {
    InputOperatorFactory inputFactory = new InputOperatorFactory(this.context);
    return inputFactory.createOperator(scan);
  }

  private int analyzeLogicalUnion(LogicalUnion union) {
    ArrayList<Integer> children = this.constructChildrenOperators(union);

    UnionOperatorFactory unionFactory = new UnionOperatorFactory(this.context);
    return unionFactory.createOperator(union, children);
  }

  private int analyzeLogicalFilter(LogicalFilter filter) {
    ArrayList<Integer> children = this.constructChildrenOperators(filter);

    FilterOperatorFactory filterFactory = new FilterOperatorFactory(this.context);
    return filterFactory.createOperator(filter, children);
  }

  private int analyzeLogicalJoin(LogicalJoin join) {
    ArrayList<Integer> children = this.constructChildrenOperators(join);

    JoinOperatorFactory joinFactory = new JoinOperatorFactory(this.context);
    return joinFactory.createOperator(join, children);
  }

  private ArrayList<Integer> analyzeLogicalProject(LogicalProject project) {
    ArrayList<Integer> children = this.constructChildrenOperators(project);

    ProjectOperatorFactory projectFactory = new ProjectOperatorFactory(this.context);
    if (projectFactory.isIdentity(project)) {
      return children;
    } else {
      ArrayList<Integer> result = new ArrayList<Integer>();
      result.add(projectFactory.createOperator(project, children));
      return result;
    }
  }

  private int analyzeLogicalAggregate(LogicalAggregate aggregate) {
    ArrayList<Integer> children = this.constructChildrenOperators(aggregate);

    AggregateOperatorFactory aggregateFactory = new AggregateOperatorFactory(this.context);
    return aggregateFactory.createOperator(aggregate, children);
  }

  // LogicalSort applies to queries that have an explicit order by, as well as unordered queries
  // that have a limit / offset clause.
  private void analyzeLogicalSort(LogicalSort sort) {
    ArrayList<Integer> children = this.constructChildrenOperators(sort);

    MatViewOperatorFactory matviewFactory = new MatViewOperatorFactory(this.context);
    matviewFactory.createOperator(sort, children);
  }

  /*
   * Plumbing so that our planning looks like simple recursion.
   */
  public ArrayList<Integer> constructChildrenOperators(RelNode operator) {
    // Add a new stack frame in which children can store their results.
    this.childrenOperators.push(new ArrayList<Integer>());

    // Actually visit children.
    visitChildren(operator);

    // Pop the stack frame and return it.
    return this.childrenOperators.pop();
  }

  /*
   * Visitor pattern inherited from calcite
   * accept(...) and visitChildren(...) end up invoking these functions.
   */
  @Override
  public RelNode visit(TableScan scan) {
    int operator = this.analyzeTableScan(scan);
    this.childrenOperators.peek().add(operator);
    return scan;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    int operator = this.analyzeLogicalUnion(union);
    this.childrenOperators.peek().add(operator);
    return union;
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    int operator = this.analyzeLogicalFilter(filter);
    this.childrenOperators.peek().add(operator);
    return filter;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    int operator = this.analyzeLogicalJoin(join);
    this.childrenOperators.peek().add(operator);
    return join;
  }

  @Override
  public RelNode visit(LogicalProject project) {
    ArrayList<Integer> operators = this.analyzeLogicalProject(project);
    this.childrenOperators.peek().addAll(operators);
    return project;
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    int operator = this.analyzeLogicalAggregate(aggregate);
    this.childrenOperators.peek().add(operator);
    return aggregate;
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    this.analyzeLogicalSort(sort);
    return sort;
  }
}
