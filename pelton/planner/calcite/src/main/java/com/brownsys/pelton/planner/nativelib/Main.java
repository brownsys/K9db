package com.brownsys.pelton.planner.nativelib;

import com.brownsys.pelton.planner.nativelib.DataFlowGraphLibrary;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class Main {
  public static void main(long ptr1, long ptr2, String query) {
    System.out.println("Java: planning query");
    System.out.println(query);
    // Create the generator interface using the pointers passed from c++.
    DataFlowGraphLibrary.DataFlowGraphGenerator generator =
        new DataFlowGraphLibrary.DataFlowGraphGenerator(ptr1, ptr2);

    int in = generator.AddInputOperator("submissions");
    int filter = generator.AddFilterOperator(in);
    generator.AddFilterOperationSigned(
        filter, 100, 3, DataFlowGraphLibrary.GREATER_THAN_OR_EQUAL);
    int out = generator.AddMatviewOperator(filter, new int[]{0});
    System.out.println("Java: Added all operators");
  }
}
