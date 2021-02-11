// These should be eventually replaced with something that is generic
// using calcite or similar.

void make_view(SharderState *state, const std::string &name,
               const std::string &query) {
  // Hardcoded schema for a submissions table from a simple-websubmit example.
  //   CREATE TABLE submissions (
  //      ID int,
  //      student_id int,
  //      assignment_id int,
  //      timestamp int,
  //    );
  // Hardcoded flow for:
  //   SELECT * FROM submissions WHERE timestamp >= 100;

  // TODO(babman): schema should be deduced from CREATE TABLE statement of
  // target(s) of query.
  const dataflow::Schema &schema = state->LogicalSchemaOf("submissions");
  

  // TODO(babman): flow should be generated from parsing query and planning it
  // with calcite.
  dataflow::DataFlowGraph graph;
  // Input operator.
  auto in = std::make_shared<dataflow::InputOperator>("submissions");
  graph.AddInputNode(in);

  // Filter operator: filter on timestamp >= 100.
  std::vector<dataflow::ColumnID> cids{0, 1, 2, 3};

  std::vector<dataflow::FilterOperator::Ops> ops{
      dataflow::FilterOperator::GreaterThanOrEqual,
      dataflow::FilterOperator::GreaterThanOrEqual,
      dataflow::FilterOperator::GreaterThanOrEqual,
      dataflow::FilterOperator::GreaterThanOrEqual};

  dataflow::Record filter_vals(schema);
  filter_vals.set_int(0, 0LL);
  filter_vals.set_int(1, 0LL);
  filter_vals.set_int(2, 0LL);
  filter_vals.set_int(3, 100LL);

  auto filter =
      std::make_shared<dataflow::FilterOperator>(cids, ops, filter_vals);
  graph.AddNode(filter, in);

  // The output materialized view operator.
  graph.AddNode(
      std::make_shared<dataflow::MatViewOperator>(schema.key_columns()),
      filter);

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  state->AddFlow(name, graph);
}

void print_view(SharderState *state, const std::string &name) {
  const dataflow::DataFlowGraph &flow = state->GetFlow(name);
  for (auto output : flow.outputs()) {
    std::cout << name << " Matview:------------" << std::endl;
    for (const auto &record : output->contents()) {
      for (size_t i = 0; i < record.schema().num_columns(); i++) {
        const void *data = record[i];
        switch (record.schema().TypeOf(i)) {
          case dataflow::DataType::kUInt:
            std::cout << *static_cast<const uint64_t *>(data) << " ";
            break;
          case dataflow::DataType::kInt:
            std::cout << *static_cast<const int64_t *>(data) << " ";
            break;
          case dataflow::DataType::kText:
            std::cout << *static_cast<const std::string *>(data) << " ";
            break;
          default:
            std::cout << "Unimplemented ";
        }
      }
      std::cout << std::endl;
    }
    std::cout << std::endl;
  }
}
