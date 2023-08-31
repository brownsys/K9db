package com.brownsys.k9db.operators;

import com.brownsys.k9db.PlanningContext;
import com.brownsys.k9db.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;

public class ProjectOperatorFactory {
  private final PlanningContext context;
  private final HashMap<Integer, Integer> k9dbSourceToTarget;
  private final HashMap<Integer, Integer> calciteToK9dbTarget;
  private int projectionsCount;
  private int targetCalciteIndex;

  public ProjectOperatorFactory(PlanningContext context) {
    this.context = context;
    this.k9dbSourceToTarget = new HashMap<Integer, Integer>();
    this.calciteToK9dbTarget = new HashMap<Integer, Integer>();
    this.projectionsCount = 0;
    this.targetCalciteIndex = -1;
  }

  public boolean isIdentity(LogicalProject project) {
    // This may be a no-op project, in which case we can just skip it!
    Permutation permutation = project.getPermutation();
    if (permutation != null) {
      // Project is a permutation (does not drop or add columns).
      if (permutation.getSourceCount() == permutation.getTargetCount()) {
        boolean isIdentity = true;
        for (int i = 0; i < permutation.getSourceCount(); i++) {
          if (permutation.getTarget(i) != i) {
            isIdentity = false;
            break;
          }
        }
        // Permutation is indeed an identity.
        if (isIdentity) {
          return true;
        }
      }
    }

    return false;
  }

  // Only called if this.isIdentity(project) returns true.
  public boolean hasAliases(LogicalProject project) {
    List<Pair<RexNode, String>> namedProjections = project.getNamedProjects();
    // Look at the type of each parent, check if the input field names are
    // identical to the projected name.
    for (RelNode parent : project.getInputs()) {
      List<String> names = parent.getRowType().getFieldNames();
      for (int i = 0; i < namedProjections.size(); i++) {
        if (!namedProjections.get(i).getValue().equals(names.get(i))) {
          return true;
        }
      }
    }
    return false;
  }

  public int createOperator(LogicalProject project, ArrayList<Integer> children) {
    assert children.size() == 1;

    int projectOperator = this.context.getGenerator().AddProjectOperator(children.get(0));

    // Stores all column indices that were already projected!
    List<Pair<RexNode, String>> namedProjections = project.getNamedProjects();
    int count = namedProjections.size();
    for (int i = 0; i < count; i++) {
      Pair<RexNode, String> item = namedProjections.get(i);
      RexNode projectedNode = item.getKey();
      String projectedName = item.getValue();

      this.targetCalciteIndex = i;
      switch (projectedNode.getKind()) {
        case INPUT_REF:
          this.addColumnProjection(projectedName, (RexInputRef) projectedNode, projectOperator);
          break;
        case LITERAL:
          this.addLiteralProjection(projectedName, (RexLiteral) projectedNode, projectOperator);
          break;
        case PLUS:
          this.addArithmeticProjection(
              projectedName,
              ((RexCall) projectedNode).getOperands(),
              DataFlowGraphLibrary.PLUS,
              projectOperator);
          break;
        case MINUS:
          this.addArithmeticProjection(
              projectedName,
              ((RexCall) projectedNode).getOperands(),
              DataFlowGraphLibrary.MINUS,
              projectOperator);
          break;
        default:
          throw new IllegalArgumentException(
              "Unsupported projection type " + projectedNode.getKind());
      }
    }

    this.context.setColumnTranslation(this.calciteToK9dbTarget, this.k9dbSourceToTarget);

    return projectOperator;
  }

  // Direct column projection.
  private void addColumnProjection(String name, RexInputRef column, int projectOperator) {
    int sourceCalciteIndex = column.getIndex();
    int sourceK9dbIndex = this.context.getK9dbIndex(sourceCalciteIndex);
    if (this.k9dbSourceToTarget.containsKey(sourceK9dbIndex)) {
      this.calciteToK9dbTarget.put(
          this.targetCalciteIndex, this.k9dbSourceToTarget.get(sourceK9dbIndex));
    } else {
      int targetK9dbIndex = this.projectionsCount;
      this.k9dbSourceToTarget.put(sourceK9dbIndex, targetK9dbIndex);
      this.calciteToK9dbTarget.put(this.targetCalciteIndex, targetK9dbIndex);
      this.projectionsCount++;
      this.addColumnProjection(name, sourceK9dbIndex, projectOperator);
    }
  }

  public void addColumnProjection(String name, int sourceK9dbIndex, int projectOperator) {
    this.context.getGenerator().AddProjectionColumn(projectOperator, name, sourceK9dbIndex);
  }

  // Literal projection.
  private void addLiteralProjection(String name, RexLiteral literal, int projectOperator) {
    int targetK9dbIndex = this.projectionsCount;
    this.projectionsCount++;

    switch (literal.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        this.calciteToK9dbTarget.put(this.targetCalciteIndex, targetK9dbIndex);
        if (name.startsWith("U_")) {
          this.context
              .getGenerator()
              .AddProjectionLiteralUInt(projectOperator, name, RexLiteral.intValue(literal));
        } else {
          this.context
              .getGenerator()
              .AddProjectionLiteralInt(projectOperator, name, RexLiteral.intValue(literal));
        }
        break;
      case VARCHAR:
      case CHAR:
        this.context
            .getGenerator()
            .AddProjectionLiteralString(projectOperator, name, RexLiteral.stringValue(literal));
        break;
      default:
        throw new IllegalArgumentException("Unsupported value type in literal projection");
    }
  }

  // Arithmetic projection.
  public void addArithmeticProjection(
      String name, List<RexNode> operands, int arithmeticEnum, int projectOperator) {
    // Must be binary.
    assert operands.size() == 2;

    // Both operands must be a column or literal.
    assert operands.get(0) instanceof RexInputRef || operands.get(0) instanceof RexLiteral;
    assert operands.get(1) instanceof RexInputRef || operands.get(1) instanceof RexLiteral;

    if (operands.get(0) instanceof RexInputRef && operands.get(1) instanceof RexInputRef) {
      RexInputRef left = (RexInputRef) operands.get(0);
      RexInputRef right = (RexInputRef) operands.get(1);
      this.addArithmeticProjectionColumns(name, left, right, arithmeticEnum, projectOperator);
    } else if (operands.get(0) instanceof RexInputRef) {
      RexInputRef left = (RexInputRef) operands.get(0);
      RexLiteral right = (RexLiteral) operands.get(1);
      this.addArithmeticProjectionLiteral(name, left, right, arithmeticEnum, projectOperator);
    } else if (operands.get(1) instanceof RexInputRef) {
      RexLiteral left = (RexLiteral) operands.get(0);
      RexInputRef right = (RexInputRef) operands.get(1);
      this.addArithmeticProjectionLiteral(name, left, right, arithmeticEnum, projectOperator);
    } else {
      throw new IllegalArgumentException(
          "Projection arithmetic expression cannot be just literal!");
    }
  }

  private void addArithmeticProjectionColumns(
      String name, RexInputRef left, RexInputRef right, int arithmeticEnum, int projectOperator) {
    int leftSourceK9dbId = this.context.getK9dbIndex(left.getIndex());
    int rightSourceK9dbId = this.context.getK9dbIndex(right.getIndex());

    int targetK9dbIndex = this.projectionsCount;
    this.projectionsCount++;
    this.calciteToK9dbTarget.put(this.targetCalciteIndex, targetK9dbIndex);
    this.context
        .getGenerator()
        .AddProjectionArithmeticColumns(
            projectOperator, name, arithmeticEnum, leftSourceK9dbId, rightSourceK9dbId);
  }

  private void addArithmeticProjectionLiteral(
      String name, RexInputRef left, RexLiteral right, int arithmeticEnum, int projectOperator) {
    int leftSourceK9dbId = this.context.getK9dbIndex(left.getIndex());
    switch (right.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        int targetK9dbIndex = this.projectionsCount;
        this.projectionsCount++;
        this.calciteToK9dbTarget.put(this.targetCalciteIndex, targetK9dbIndex);
        this.context
            .getGenerator()
            .AddProjectionArithmeticLiteralRight(
                projectOperator,
                name,
                arithmeticEnum,
                leftSourceK9dbId,
                RexLiteral.intValue(right));
        break;
      default:
        throw new IllegalArgumentException("Unsupported value type in literal projection");
    }
  }

  private void addArithmeticProjectionLiteral(
      String name, RexLiteral left, RexInputRef right, int arithmeticEnum, int projectOperator) {
    int rightSourceK9dbId = this.context.getK9dbIndex(right.getIndex());
    switch (left.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        int targetK9dbIndex = this.projectionsCount;
        this.projectionsCount++;
        this.calciteToK9dbTarget.put(this.targetCalciteIndex, targetK9dbIndex);
        this.context
            .getGenerator()
            .AddProjectionArithmeticLiteralLeft(
                projectOperator,
                name,
                arithmeticEnum,
                RexLiteral.intValue(left),
                rightSourceK9dbId);
        break;
      default:
        throw new IllegalArgumentException("Unsupported value type in literal projection");
    }
  }
}
