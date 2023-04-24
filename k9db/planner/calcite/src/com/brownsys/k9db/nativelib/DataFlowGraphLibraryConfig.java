package com.brownsys.k9db.nativelib;

import org.bytedeco.javacpp.annotation.Platform;
import org.bytedeco.javacpp.annotation.Properties;
import org.bytedeco.javacpp.tools.Info;
import org.bytedeco.javacpp.tools.InfoMap;
import org.bytedeco.javacpp.tools.InfoMapper;

@Properties(
    value =
        @Platform(
            include = {
              "k9db/dataflow/types.h",
              "k9db/dataflow/ops/aggregate_enum.h",
              "k9db/dataflow/ops/equijoin_enum.h",
              "k9db/dataflow/ops/filter_enum.h",
              "k9db/dataflow/ops/project_enum.h",
              "k9db/sqlast/ast_schema_enums.h",
              "k9db/dataflow/generator.h"
            }),
    target = "com.brownsys.k9db.nativelib.DataFlowGraphLibrary")
public class DataFlowGraphLibraryConfig implements InfoMapper {
  public void map(InfoMap infoMap) {
    infoMap.put(new Info("std::vector<std::string>").pointerTypes("StringVector").define());
  }
}
