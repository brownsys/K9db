package com.brownsys.pelton;

import com.brownsys.pelton.nativelib.DataFlowGraphLibrary;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class QueryPlanner {
  public static SchemaPlus ConstructSchema(DataFlowGraphLibrary.DataFlowGraphGenerator generator) {
    System.out.println("Construct Schema");
    SchemaPlus schema = Frameworks.createRootSchema(true);
    return schema;
  }


  public static void plan(long ptr1, long ptr2, String query) {
    System.out.println("Java: planning query");
    System.out.println(query);
    // Create the generator interface using the pointers passed from c++.
    DataFlowGraphLibrary.DataFlowGraphGenerator generator =
        new DataFlowGraphLibrary.DataFlowGraphGenerator(ptr1, ptr2);

    SchemaPlus schema = ConstructSchema(generator);

    int in = generator.AddInputOperator("submissions");
    int filter = generator.AddFilterOperator(in);
    generator.AddFilterOperationSigned(filter, 100, 3, DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL);
    int out = generator.AddMatviewOperator(filter, new int[] {0});
    System.out.println("Java: Added all operators");
  }
}
