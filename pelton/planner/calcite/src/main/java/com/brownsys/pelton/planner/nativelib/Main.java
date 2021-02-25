package com.brownsys.pelton.planner.nativelib;

import com.brownsys.pelton.planner.nativelib.DataFlowGraphLibrary;
import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;

public class Main {
  public static void main(long ptr1, long ptr2) {
    DataFlowGraphLibrary.DataFlowGraphGenerator generator =
        new DataFlowGraphLibrary.DataFlowGraphGenerator(ptr1, ptr2);
    generator.AddInputOperator("hello-world-table");
  }
}
