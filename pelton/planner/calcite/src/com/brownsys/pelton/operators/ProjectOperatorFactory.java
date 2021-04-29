package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;

public class ProjectOperatorFactory {
  private final PlanningContext context;
  private final HashMap<Integer, Integer> peltonSourceToTarget;
  private final HashMap<Integer, Integer> calciteToPeltonTarget;
  private int projectionsCount;
  private int targetCalciteIndex;

  public ProjectOperatorFactory(PlanningContext context) {
    this.context = context;
    this.peltonSourceToTarget = new HashMap<Integer, Integer>();
    this.calciteToPeltonTarget = new HashMap<Integer, Integer>();
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

    this.context.setColumnTranslation(this.calciteToPeltonTarget, this.peltonSourceToTarget);

    return projectOperator;
  }

  // Direct column projection.
  private void addColumnProjection(String name, RexInputRef column, int projectOperator) {
    int sourceCalciteIndex = column.getIndex();
    int sourcePeltonIndex = this.context.getPeltonIndex(sourceCalciteIndex);
    if (this.peltonSourceToTarget.containsKey(sourcePeltonIndex)) {
      this.calciteToPeltonTarget.put(
          this.targetCalciteIndex, this.peltonSourceToTarget.get(sourcePeltonIndex));
    } else {
      int targetPeltonIndex = this.projectionsCount;
      this.peltonSourceToTarget.put(sourcePeltonIndex, targetPeltonIndex);
      this.calciteToPeltonTarget.put(this.targetCalciteIndex, targetPeltonIndex);
      this.projectionsCount++;
      this.addColumnProjection(name, sourcePeltonIndex, projectOperator);
    }
  }

  public void addColumnProjection(String name, int sourcePeltonIndex, int projectOperator) {
    this.context.getGenerator().AddProjectionColumn(projectOperator, name, sourcePeltonIndex);
  }

  // Literal projection.
  private void addLiteralProjection(String name, RexLiteral literal, int projectOperator) {
    int targetPeltonIndex = this.projectionsCount;
    this.projectionsCount++;

    switch (literal.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        this.calciteToPeltonTarget.put(this.targetCalciteIndex, targetPeltonIndex);
        this.context
            .getGenerator()
            .AddProjectionLiteralInt(projectOperator, name, RexLiteral.intValue(literal));
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
    int leftSourcePeltonId = this.context.getPeltonIndex(left.getIndex());
    int rightSourcePeltonId = this.context.getPeltonIndex(right.getIndex());

    int targetPeltonIndex = this.projectionsCount;
    this.projectionsCount++;
    this.calciteToPeltonTarget.put(this.targetCalciteIndex, targetPeltonIndex);
    this.context
        .getGenerator()
        .AddProjectionArithmeticColumns(
            projectOperator, name, leftSourcePeltonId, rightSourcePeltonId, arithmeticEnum);
  }

  private void addArithmeticProjectionLiteral(
      String name, RexInputRef left, RexLiteral right, int arithmeticEnum, int projectOperator) {
    int leftSourcePeltonId = this.context.getPeltonIndex(left.getIndex());
    switch (right.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        int targetPeltonIndex = this.projectionsCount;
        this.projectionsCount++;
        this.calciteToPeltonTarget.put(this.targetCalciteIndex, targetPeltonIndex);
        this.context
            .getGenerator()
            .AddProjectionArithmeticLiteralLeft(
                projectOperator,
                name,
                leftSourcePeltonId,
                RexLiteral.intValue(right),
                arithmeticEnum);
        break;
      default:
        throw new IllegalArgumentException("Unsupported value type in literal projection");
    }
  }

  private void addArithmeticProjectionLiteral(
      String name, RexLiteral left, RexInputRef right, int arithmeticEnum, int projectOperator) {
    int rightSourcePeltonId = this.context.getPeltonIndex(right.getIndex());
    switch (left.getTypeName()) {
      case DECIMAL:
      case INTEGER:
        int targetPeltonIndex = this.projectionsCount;
        this.projectionsCount++;
        this.calciteToPeltonTarget.put(this.targetCalciteIndex, targetPeltonIndex);
        this.context
            .getGenerator()
            .AddProjectionArithmeticLiteralRight(
                projectOperator,
                name,
                RexLiteral.intValue(left),
                rightSourcePeltonId,
                arithmeticEnum);
        break;
      default:
        throw new IllegalArgumentException("Unsupported value type in literal projection");
    }
  }
}
