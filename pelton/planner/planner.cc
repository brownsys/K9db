#include <cstring>
#include <iostream>
#include <memory>
#include <utility>

#include "glog/logging.h"
#include "jni.h"
#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"

#define MAVEN_PATH "wrapper.runfiles/maven/v1/"
#define PELTON_PATH "wrapper.runfiles/pelton/"
#define JAVACPP_PATH \
  "https/jcenter.bintray.com/org/bytedeco/javacpp/1.5.4/javacpp-1.5.4.jar"
#define NATIVELIB_PATH                                                \
  "pelton/planner/calcite/src/main/java/com/brownsys/pelton/planner/" \
  "nativelib/"
#define JAVA_MAIN "Main.jar"

#define CLASS_PATH_ARG "-Djava.class.path="
#define LIBRARY_PATH_ARG "-Djava.library.path="

namespace pelton {
namespace planner {

// Internal helpers for starting a JVM.
namespace {
// Specify the class path and library path arguments to the JVM
// so that it can locate our calcite java package, and can load
// the needed C++ JNI interfaces.
void InitializeJVMArgs(JavaVMInitArgs *args, const std::string &dir) {
  // Path to jars or so libraries required by the jvm.
  std::string javacpp = dir + "/" + MAVEN_PATH + JAVACPP_PATH;
  std::string javamain = dir + "/" + PELTON_PATH + NATIVELIB_PATH + JAVA_MAIN;
  std::string generator_so = dir + "/" + PELTON_PATH + NATIVELIB_PATH;

  // Class path and library path arguments to the JVM.
  std::string cp = CLASS_PATH_ARG + javacpp + ":" + javamain;
  std::string lp = LIBRARY_PATH_ARG + generator_so;

  // Create c-style strings that jvm understands.
  char *cp_ptr = new char[cp.size()];
  char *lp_ptr = new char[lp.size()];
  strcpy(cp_ptr, cp.c_str());
  strcpy(lp_ptr, lp.c_str());

  // Finally, add all these arguments to *args.
  JavaVMOption options[2];
  options[0].optionString = cp_ptr;
  options[1].optionString = lp_ptr;
  args->version = JNI_VERSION_10;
  args->nOptions = 2;
  args->options = options;
  args->ignoreUnrecognized = JNI_TRUE;
  JNI_GetDefaultJavaVMInitArgs(args);
}
// Start a JVM embedded in this process.
std::pair<JavaVM *, JNIEnv *> StartJVM(const std::string &dir) {
  // These will point to the JVM and JNI env after initialization.
  JavaVM *jvm;
  JNIEnv *env;

  // Start JVM.
  JavaVMInitArgs jvm_args;
  InitializeJVMArgs(&jvm_args, dir);
  jint res = JNI_CreateJavaVM(&jvm, reinterpret_cast<void **>(&env), &jvm_args);
  CHECK(res == 0) << "failed to start JVM";

  // All good.
  return std::make_pair(std::move(jvm), std::move(env));
}
// Shutdown a running JVM.
void StopJVM(JavaVM *jvm) { jvm->DestroyJavaVM(); }

}  // namespace

// Given a query, use calcite to plan its execution, and generate
// a DataFlowGraph with all the operators from that plan.
dataflow::DataFlowGraph PlanGraph(dataflow::DataflowState *state,
                                  const std::string &query,
                                  const std::string &dir) {
  // Start a JVM.
  auto [jvm, env] = StartJVM(dir);

  // First get the java/calcite entry point class.
  jclass clazz = env->FindClass("com/brownsys/pelton/planner/nativelib/Main");

  // Get the (static) entry method from that class.
  jmethodID main = env->GetStaticMethodID(clazz, "main", "(JJ)V");

  // Create the required shared state to pass to the java code.
  dataflow::DataFlowGraph graph;
  jlong graph_ptr = reinterpret_cast<jlong>(&graph);
  jlong state_ptr = reinterpret_cast<jlong>(state);

  // Call the method on the object
  env->CallStaticVoidMethod(clazz, main, graph_ptr, state_ptr);
  if (env->ExceptionCheck()) {  // Handle any exception.
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(FATAL) << "--- Java exception encountered during planning";
  }

  // Stop the JVM.
  StopJVM(jvm);

  // Return the graph.
  return graph;
}

}  // namespace planner
}  // namespace pelton

int main(int argc, char **argv) {
  // Make a dummy query.
  std::string dummy_query = "";

  // Get the directory of the script.
  size_t len = strlen(argv[0]);
  std::string dir(argv[0], len - 8);

  // Create a dummy state.
  pelton::dataflow::DataflowState state;
  state.AddTableSchema("hello-world-table",
                       pelton::dataflow::SchemaOwner({}, {}, {}));

  // Plan the graph via calcite.
  pelton::dataflow::DataFlowGraph graph =
      pelton::planner::PlanGraph(&state, dummy_query, dir);

  // Check that the graph is what we expect!
  std::shared_ptr<pelton::dataflow::Operator> op = graph.GetNode(0);
  std::shared_ptr<pelton::dataflow::InputOperator> input =
      graph.inputs().at("hello-world-table");
  std::cout << input->input_name() << std::endl;
  CHECK(op.get() == input.get()) << "Operator and InputOperator are different";

  // All good.
  return 0;
}
