package com.brownsys.pelton.nativelib;

import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.Info;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

@Properties(
    value =
        @Platform(
            include = {
              "pelton/dataflow/types.h",
              "pelton/dataflow/ops/filter_enum.h",
              "pelton/dataflow/ops/aggregate_enum.h",
              "pelton/sqlast/ast_schema_enums.h",
              "pelton/dataflow/generator.h"
            },
            link = {"generator"}),
    target = "com.brownsys.pelton.nativelib.DataFlowGraphLibrary")
public class DataFlowGraphLibraryConfig implements InfoMapper {
  public void map(InfoMap infoMap) {
    infoMap.put(new Info("std::vector<std::string>").pointerTypes("StringVector").define());
  }
}
