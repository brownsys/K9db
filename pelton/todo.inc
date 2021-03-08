// These should be eventually replaced with something that is generic
// using calcite or similar.
/*
void make_view(Connection *connection, const std::string &name,
               const std::string &query) {
  // Hardcoded schema for a submissions table from a simple-websubmit example.
  //   CREATE TABLE submissions (
  //      ID int PRIMARY KEY,
  //      student_id int,
  //      assignment_id int,
  //      timestamp int,
  //    );
  // Hardcoded flow for:
  //   SELECT * FROM submissions WHERE timestamp >= 100;
  dataflow::SchemaRef schema =
      connection->GetDataFlowState()->GetTableSchema("submissions");

  // TODO(babman): flow should be generated from parsing query and planning it
  // with calcite.
  // Input operator.
  auto in = std::make_shared<dataflow::InputOperator>("submissions", schema);

  // Filter operator: filter on timestamp >= 100.
  auto filter = std::make_shared<dataflow::FilterOperator>();
  filter->AddOperation(
      100L, 3, dataflow::FilterOperator::Operation::GREATER_THAN_OR_EQUAL);

  // Materialized view output.
  std::vector<dataflow::ColumnID> keys = {0};
  auto matview = std::make_shared<dataflow::MatViewOperator>(keys);

  // Construct graph.
  dataflow::DataFlowGraph graph;
  graph.AddInputNode(in);
  graph.AddNode(filter, in);
  graph.AddOutputOperator(matview, filter);

  // Add The flow to state so that data is fed into it on INSERT/UPDATE/DELETE.
  connection->GetDataFlowState()->AddFlow(name, graph);
}
*/

void print_view(Connection *connection, const std::string &name) {
  const dataflow::DataFlowGraph &flow =
      connection->GetDataFlowState()->GetFlow(name);
  for (auto output : flow.outputs()) {
    std::cout << name << " Matview:------------" << std::endl;
    std::cout << output->output_schema() << std::endl;
    for (const auto &record : *output) {
      std::cout << record << std::endl;
    }
    std::cout << std::endl;
  }
}
