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
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Permutation;

public class ProjectOperatorFactory {
  private final PlanningContext context;

  public ProjectOperatorFactory(PlanningContext context) {
    this.context = context;
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
    HashMap<Integer, Integer> peltonSourceToTarget = new HashMap<Integer, Integer>();
    HashMap<Integer, Integer> calciteToPeltonTarget = new HashMap<Integer, Integer>();
    int targetPeltonIndex = 0;

    List<Pair<RexNode, String>> namedProjections = project.getNamedProjects();
    for (int targetCalciteIndex = 0;
        targetCalciteIndex < namedProjections.size();
        targetCalciteIndex++) {

      Pair<RexNode, String> item = namedProjections.get(targetCalciteIndex);
      RexNode projectedNode = item.getKey();
      String projectedName = item.getValue();

      switch (projectedNode.getKind()) {
        case INPUT_REF:
          {
            int sourceCalciteIndex = ((RexInputRef) projectedNode).getIndex();
            int sourcePeltonIndex = this.context.getPeltonIndex(sourceCalciteIndex);
            if (peltonSourceToTarget.containsKey(sourcePeltonIndex)) {
              calciteToPeltonTarget.put(
                  targetCalciteIndex, peltonSourceToTarget.get(sourcePeltonIndex));
            } else {
              peltonSourceToTarget.put(sourcePeltonIndex, targetPeltonIndex);
              calciteToPeltonTarget.put(targetCalciteIndex, targetPeltonIndex);
              targetPeltonIndex++;
              this.context
                  .getGenerator()
                  .AddProjectionColumn(projectOperator, projectedName, sourcePeltonIndex);
            }
            break;
          }
        case LITERAL:
          {
            RexLiteral value = (RexLiteral) projectedNode;
            switch (value.getTypeName()) {
              case DECIMAL:
              case INTEGER:
                calciteToPeltonTarget.put(targetCalciteIndex, targetPeltonIndex);
                targetPeltonIndex++;
                this.context
                    .getGenerator()
                    .AddProjectionLiteralSigned(
                        projectOperator,
                        projectedName,
                        RexLiteral.intValue(value),
                        DataFlowGraphLibrary.LITERAL);
                break;
              default:
                throw new IllegalArgumentException("Unsupported value type in literal projection");
            }
            break;
          }
        case PLUS:
        case MINUS:
          {
            int arithmeticEnum =
                projectedNode.getKind() == SqlKind.PLUS
                    ? DataFlowGraphLibrary.PLUS
                    : DataFlowGraphLibrary.MINUS;

            RexCall operation = (RexCall) projectedNode;
            List<RexNode> operands = operation.getOperands();

            // Must be binary.
            assert operands.size() == 2;
            // Left side must be a column.
            assert operands.get(0) instanceof RexInputRef;
            // Right side must be a column or literal.
            assert operands.get(1) instanceof RexInputRef || operands.get(1) instanceof RexLiteral;

            // Update column index in case of de-duplication
            int leftSourceCalciteId = ((RexInputRef) operands.get(0)).getIndex();
            int leftSourcePeltonId = this.context.getPeltonIndex(leftSourceCalciteId);

            if (operands.get(1) instanceof RexInputRef) {
              int rightSourceCalciteId = ((RexInputRef) operands.get(1)).getIndex();
              int rightSourcePeltonId = this.context.getPeltonIndex(rightSourceCalciteId);
              calciteToPeltonTarget.put(targetCalciteIndex, targetPeltonIndex);
              targetPeltonIndex++;
              this.context
                  .getGenerator()
                  .AddProjectionArithmeticWithLiteralUnsignedOrColumn(
                      projectOperator,
                      projectedName,
                      leftSourcePeltonId,
                      arithmeticEnum,
                      rightSourcePeltonId,
                      DataFlowGraphLibrary.ARITHMETIC_WITH_COLUMN);
            } else {
              RexLiteral rightValue = (RexLiteral) operands.get(1);
              switch (rightValue.getTypeName()) {
                case DECIMAL:
                case INTEGER:
                  calciteToPeltonTarget.put(targetCalciteIndex, targetPeltonIndex);
                  targetPeltonIndex++;
                  this.context
                      .getGenerator()
                      .AddProjectionArithmeticWithLiteralSigned(
                          projectOperator,
                          projectedName,
                          leftSourcePeltonId,
                          arithmeticEnum,
                          RexLiteral.intValue(rightValue),
                          DataFlowGraphLibrary.ARITHMETIC_WITH_LITERAL);
                  break;
                default:
                  throw new IllegalArgumentException(
                      "Unsupported value type in literal projection");
              }
            }
            break;
          }
        default:
          throw new IllegalArgumentException(
              "Unsupported projection type " + projectedNode.getKind());
      }
    }

    this.context.setColumnTranslation(calciteToPeltonTarget, peltonSourceToTarget);

    return projectOperator;
  }
}
