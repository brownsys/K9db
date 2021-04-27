package com.brownsys.pelton.operators;

import com.brownsys.pelton.PlanningContext;
import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import java.util.ArrayList;
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

  private int UpdateIndexIfRequired(int columnId, ArrayList<Integer> duplicateColumns) {
    for (Integer dupCol : duplicateColumns) {
      if (columnId > dupCol) {
        columnId = columnId - 1;
      }
    }
    return columnId;
  }

  public int createOperator(LogicalProject project, ArrayList<Integer> children) {
    assert children.size() == 1;

    int projectOperator = this.context.getGenerator().AddProjectOperator(children.get(0));

    ArrayList<Integer> duplicateColumns = new ArrayList<Integer>();
    boolean deduplicated = false;
    for (Pair<RexNode, String> item : project.getNamedProjects()) {
      switch (item.getKey().getKind()) {
        case INPUT_REF:
          int projectedCid = ((RexInputRef) item.getKey()).getIndex();
          // Check if this column as to be deduplicated
          for (Integer cid : duplicateColumns) {
            if (cid == projectedCid) {
              deduplicated = true;
              continue;
            }
          }
          // Track potential duplicates (if any)
          for (ArrayList<Integer> entry : this.context.joinDuplicateColumns) {
            if (entry.get(0) == projectedCid) {
              duplicateColumns.add(entry.get(1));
            }
          }
          // Update column index if deduplication has occured
          if (deduplicated) projectedCid = UpdateIndexIfRequired(projectedCid, duplicateColumns);
          this.context
              .getGenerator()
              .AddProjectionColumn(projectOperator, item.getValue(), projectedCid);
          break;
        case LITERAL:
          RexLiteral value = (RexLiteral) item.getKey();
          switch (value.getTypeName()) {
            case DECIMAL:
            case INTEGER:
              this.context
                  .getGenerator()
                  .AddProjectionLiteralSigned(
                      projectOperator,
                      item.getValue(),
                      RexLiteral.intValue(value),
                      DataFlowGraphLibrary.LITERAL);
              break;
            default:
              throw new IllegalArgumentException("Unsupported value type in literal projection");
          }
          break;
          // TODO(Ishan): Add support for desired operations
        case PLUS:
        case MINUS:
          RexCall operation = (RexCall) item.getKey();
          List<RexNode> operands = operation.getOperands();
          assert operands.size() == 2;
          assert operands.get(0) instanceof RexInputRef;
          assert operands.get(1) instanceof RexInputRef || operands.get(1) instanceof RexLiteral;
          Integer leftColumnId = ((RexInputRef) operands.get(0)).getIndex();
          int arithmeticEnum = -1;
          if (item.getKey().getKind() == SqlKind.PLUS) {
            arithmeticEnum = DataFlowGraphLibrary.PLUS;
          } else if (item.getKey().getKind() == SqlKind.MINUS) {
            arithmeticEnum = DataFlowGraphLibrary.MINUS;
          }
          // Update column index if deduplication has occured
          if (deduplicated) leftColumnId = UpdateIndexIfRequired(leftColumnId, duplicateColumns);
          if (operands.get(1) instanceof RexInputRef) {
            Integer rightColumnId = ((RexInputRef) operands.get(1)).getIndex();
            // Update column index if deduplication has occured
            if (deduplicated)
              rightColumnId = UpdateIndexIfRequired(rightColumnId, duplicateColumns);
            this.context
                .getGenerator()
                .AddProjectionArithmeticWithLiteralUnsignedOrColumn(
                    projectOperator,
                    item.getValue(),
                    leftColumnId,
                    arithmeticEnum,
                    rightColumnId,
                    DataFlowGraphLibrary.ARITHMETIC_WITH_COLUMN);
          } else {
            RexLiteral rightValue = (RexLiteral) operands.get(1);
            switch (rightValue.getTypeName()) {
              case DECIMAL:
              case INTEGER:
                this.context
                    .getGenerator()
                    .AddProjectionArithmeticWithLiteralSigned(
                        projectOperator,
                        item.getValue(),
                        leftColumnId,
                        arithmeticEnum,
                        RexLiteral.intValue(rightValue),
                        DataFlowGraphLibrary.ARITHMETIC_WITH_LITERAL);
                break;
              default:
                throw new IllegalArgumentException("Unsupported value type in literal projection");
            }
            break;
          }
          break;
        default:
          throw new IllegalArgumentException("Unsupported projection type");
      }
    }

    return projectOperator;
  }
}
