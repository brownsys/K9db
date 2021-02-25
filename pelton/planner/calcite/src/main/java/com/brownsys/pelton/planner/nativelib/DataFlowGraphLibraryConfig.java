package com.brownsys.pelton.planner.nativelib;

import org.bytedeco.javacpp.*;
import org.bytedeco.javacpp.annotation.*;
import org.bytedeco.javacpp.tools.*;

@Properties(
    value = @Platform(
        include = {
          "pelton/dataflow/types.h",
          "pelton/dataflow/ops/filter_enum.h",
          "pelton/dataflow/generator.h"},
        link = {"generator_so"}
    ),
    target = "com.brownsys.pelton.planner.nativelib.DataFlowGraphLibrary"
)
public class DataFlowGraphLibraryConfig implements InfoMapper {
    public void map(InfoMap infoMap) {
    }
}
