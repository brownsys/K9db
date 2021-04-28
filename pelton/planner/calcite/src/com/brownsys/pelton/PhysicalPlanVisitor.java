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
import org.apache.calcite.util.Pair;

public class PhysicalPlanVisitor extends RelShuttleImpl {
  private final DataFlowGraphLibrary.DataFlowGraphGenerator generator;
  private final Stack<ArrayList<Integer>> childrenOperators;
  private final Stack<ArrayList<PlanningContext>> childrenContexts;

  public PhysicalPlanVisitor(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    this.generator = generator;
    this.childrenOperators = new Stack<ArrayList<Integer>>();
    this.childrenContexts = new Stack<ArrayList<PlanningContext>>();
  }

  /*
   * Main entry point. This is called by QueryPlanner.java to transform Calcite's query
   * plan to a pelton data flow.
   */
  public void populateGraph(RelNode plan) {
    this.childrenOperators.push(new ArrayList<Integer>());
    this.childrenContexts.push(new ArrayList<PlanningContext>());

    // Start visiting.
    plan.accept(this);

    // Check if a mat view operator was already created, otherwise create one.
    ArrayList<Integer> children = this.childrenOperators.pop();
    ArrayList<PlanningContext> contexts = this.childrenContexts.pop();
    if (children.size() > 0) {
      assert children.size() == 1;
      assert contexts.size() == 1;

      MatViewOperatorFactory matviewFactory = new MatViewOperatorFactory(contexts.get(0));
      matviewFactory.createOperator(children);

      assert this.childrenOperators.isEmpty();
    }
  }

  /*
   * Actual planning code.
   */

  private Pair<Integer, PlanningContext> analyzeTableScan(TableScan scan) {
    PlanningContext context = new PlanningContext(this.generator);
    InputOperatorFactory inputFactory = new InputOperatorFactory(context);
    return new Pair<>(inputFactory.createOperator(scan), context);
  }

  private Pair<Integer, PlanningContext> analyzeLogicalUnion(LogicalUnion union) {
    Pair<ArrayList<Integer>, PlanningContext> pair =
        this.constructChildrenOperators(union, PlanningContext.MergeOperation.UNION);
    ArrayList<Integer> children = pair.left;
    PlanningContext context = pair.right;

    UnionOperatorFactory unionFactory = new UnionOperatorFactory(context);
    return new Pair<>(unionFactory.createOperator(union, children), context);
  }

  private Pair<Integer, PlanningContext> analyzeLogicalFilter(LogicalFilter filter) {
    Pair<ArrayList<Integer>, PlanningContext> pair =
        this.constructChildrenOperators(filter, PlanningContext.MergeOperation.OTHER);
    ArrayList<Integer> children = pair.left;
    PlanningContext context = pair.right;

    FilterOperatorFactory filterFactory = new FilterOperatorFactory(context);
    return new Pair<>(filterFactory.createOperator(filter, children), context);
  }

  private Pair<Integer, PlanningContext> analyzeLogicalJoin(LogicalJoin join) {
    Pair<ArrayList<Integer>, PlanningContext> pair =
        this.constructChildrenOperators(join, PlanningContext.MergeOperation.JOIN);
    ArrayList<Integer> children = pair.left;
    PlanningContext context = pair.right;

    JoinOperatorFactory joinFactory = new JoinOperatorFactory(context);
    return new Pair<>(joinFactory.createOperator(join, children), context);
  }

  private Pair<ArrayList<Integer>, PlanningContext> analyzeLogicalProject(LogicalProject project) {
    Pair<ArrayList<Integer>, PlanningContext> pair =
        this.constructChildrenOperators(project, PlanningContext.MergeOperation.OTHER);
    ArrayList<Integer> children = pair.left;
    PlanningContext context = pair.right;

    ProjectOperatorFactory projectFactory = new ProjectOperatorFactory(context);
    if (projectFactory.isIdentity(project)) {
      return new Pair<>(children, context);
    } else {
      ArrayList<Integer> result = new ArrayList<Integer>();
      result.add(projectFactory.createOperator(project, children));
      return new Pair<>(result, context);
    }
  }

  private Pair<Integer, PlanningContext> analyzeLogicalAggregate(LogicalAggregate aggregate) {
    Pair<ArrayList<Integer>, PlanningContext> pair =
        this.constructChildrenOperators(aggregate, PlanningContext.MergeOperation.OTHER);
    ArrayList<Integer> children = pair.left;
    PlanningContext context = pair.right;

    AggregateOperatorFactory aggregateFactory = new AggregateOperatorFactory(context);
    return new Pair<>(aggregateFactory.createOperator(aggregate, children), context);
  }

  // LogicalSort applies to queries that have an explicit order by, as well as unordered queries
  // that have a limit / offset clause.
  private void analyzeLogicalSort(LogicalSort sort) {
    Pair<ArrayList<Integer>, PlanningContext> pair =
        this.constructChildrenOperators(sort, PlanningContext.MergeOperation.OTHER);
    ArrayList<Integer> children = pair.left;
    PlanningContext context = pair.right;

    MatViewOperatorFactory matviewFactory = new MatViewOperatorFactory(context);
    matviewFactory.createOperator(sort, children);
  }

  /*
   * Plumbing so that our planning looks like simple recursion.
   */
  public Pair<ArrayList<Integer>, PlanningContext> constructChildrenOperators(
      RelNode operator, PlanningContext.MergeOperation op) {
    // Add a new stack frame in which children can store their results.
    this.childrenOperators.push(new ArrayList<Integer>());
    this.childrenContexts.push(new ArrayList<PlanningContext>());

    // Actually visit children.
    visitChildren(operator);

    // Pop the stack frame and return it.
    ArrayList<PlanningContext> contexts = this.childrenContexts.pop();
    PlanningContext context = contexts.get(0);
    for (int i = 1; i < contexts.size(); i++) {
      context.merge(op, contexts.get(i));
    }

    return new Pair<>(this.childrenOperators.pop(), context);
  }

  /*
   * Visitor pattern inherited from calcite
   * accept(...) and visitChildren(...) end up invoking these functions.
   */
  @Override
  public RelNode visit(TableScan scan) {
    Pair<Integer, PlanningContext> pair = this.analyzeTableScan(scan);
    int operator = pair.left;
    PlanningContext context = pair.right;

    this.childrenOperators.peek().add(operator);
    this.childrenContexts.peek().add(context);
    return scan;
  }

  @Override
  public RelNode visit(LogicalUnion union) {
    Pair<Integer, PlanningContext> pair = this.analyzeLogicalUnion(union);
    int operator = pair.left;
    PlanningContext context = pair.right;

    this.childrenOperators.peek().add(operator);
    this.childrenContexts.peek().add(context);
    return union;
  }

  @Override
  public RelNode visit(LogicalFilter filter) {
    Pair<Integer, PlanningContext> pair = this.analyzeLogicalFilter(filter);
    int operator = pair.left;
    PlanningContext context = pair.right;

    this.childrenOperators.peek().add(operator);
    this.childrenContexts.peek().add(context);
    return filter;
  }

  @Override
  public RelNode visit(LogicalJoin join) {
    Pair<Integer, PlanningContext> pair = this.analyzeLogicalJoin(join);
    int operator = pair.left;
    PlanningContext context = pair.right;

    this.childrenOperators.peek().add(operator);
    this.childrenContexts.peek().add(context);
    return join;
  }

  @Override
  public RelNode visit(LogicalProject project) {
    Pair<ArrayList<Integer>, PlanningContext> pair = this.analyzeLogicalProject(project);
    ArrayList<Integer> operators = pair.left;
    PlanningContext context = pair.right;

    this.childrenOperators.peek().addAll(operators);
    this.childrenContexts.peek().add(context);
    return project;
  }

  @Override
  public RelNode visit(LogicalAggregate aggregate) {
    Pair<Integer, PlanningContext> pair = this.analyzeLogicalAggregate(aggregate);
    int operator = pair.left;
    PlanningContext context = pair.right;

    this.childrenOperators.peek().add(operator);
    this.childrenContexts.peek().add(context);
    return aggregate;
  }

  @Override
  public RelNode visit(LogicalSort sort) {
    this.analyzeLogicalSort(sort);
    return sort;
  }
}
