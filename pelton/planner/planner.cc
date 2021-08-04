#include "pelton/planner/planner.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
#include "pelton/util/perf.h"
// NOLINTNEXTLINE
#include "jni.h"

#define CALCITE_PATH "pelton/planner/calcite/src/com/brownsys/pelton/"
#define JAR_NAME "QueryPlanner_deploy.jar"
#define NATIVELIB_PATH "nativelib/"

#define CLASS_PATH_ARG "-Djava.class.path="
#define LIBRARY_PATH_ARG "-Djava.library.path="

// Configure java logging level according to compilation mode/config.
#ifndef PELTON_OPT
#define PELTON_JAVA_DEBUG true
#else
#define PELTON_JAVA_DEBUG false
#endif  // PELTON_OPT

namespace pelton {
namespace planner {

// Internal helpers for starting a JVM.
namespace {

// Start a JVM embedded in this process.
std::pair<JavaVM *, JNIEnv *> StartJVM(bool run = true) {
  // JVM is created once and kept until no longer needed.
  static JavaVM *jvm = nullptr;
  static JNIEnv *env = nullptr;

  if (run && jvm == nullptr) {
    // Specify the class path and library path arguments to the JVM
    // so that it can locate our calcite java package, and can load
    // the needed C++ JNI interfaces.
    JavaVMInitArgs jvm_args;

    // Class path and library path arguments to the JVM.
    std::string cp = CLASS_PATH_ARG CALCITE_PATH JAR_NAME;
    std::string lp = LIBRARY_PATH_ARG CALCITE_PATH NATIVELIB_PATH;

    // Create c-style strings that jvm understands.
    char *cp_ptr = new char[cp.size() + 1];
    char *lp_ptr = new char[lp.size() + 1];
    memcpy(cp_ptr, cp.c_str(), cp.size() + 1);
    memcpy(lp_ptr, lp.c_str(), lp.size() + 1);

    // Finally, add all these arguments to *args.
    JavaVMOption options[2];
    options[0].optionString = cp_ptr;
    options[1].optionString = lp_ptr;

    // options[2].optionString = "-verbose:jni";
    jvm_args.version = JNI_VERSION_1_8;
    jvm_args.options = options;
    jvm_args.nOptions = 2;
    jvm_args.ignoreUnrecognized = JNI_TRUE;

    // Start the JVM.
    CHECK(!JNI_CreateJavaVM(&jvm, reinterpret_cast<void **>(&env), &jvm_args))
        << "failed to start JVM";
  }

  // All good.
  return std::make_pair(std::move(jvm), std::move(env));
}

}  // namespace

// Given a query, use calcite to plan its execution, and generate
// a DataFlowGraph with all the operators from that plan.
dataflow::DataFlowGraph PlanGraph(dataflow::DataFlowState *state,
                                  const std::string &query) {
  perf::Start("planner");
  // Start a JVM.
  auto [jvm, env] = StartJVM();

  // First get the java/calcite entry point class.
  jclass QueryPlannerClass = env->FindClass("com/brownsys/pelton/QueryPlanner");

  // Get the (static) entry method from that class.
  const char *sig = "(JJLjava/lang/String;Z)V";
  jmethodID planMethod = env->GetStaticMethodID(QueryPlannerClass, "plan", sig);

  // Create the required shared state to pass to the java code.
  dataflow::DataFlowGraph graph;
  jlong graph_jptr = reinterpret_cast<jlong>(&graph);
  jlong state_jptr = reinterpret_cast<jlong>(state);
  jstring query_jstr = env->NewStringUTF(query.c_str());

  // Call the method on the object
  env->CallStaticVoidMethod(QueryPlannerClass, planMethod, graph_jptr,
                            state_jptr, query_jstr, PELTON_JAVA_DEBUG);
  if (env->ExceptionCheck()) {  // Handle any exception.
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(FATAL) << "--- Java exception encountered during planning";
  }

  // Free up the created jstring.
  env->DeleteLocalRef(query_jstr);
  perf::End("planner");

  // Return the graph.
  return graph;
}

// Shutdown the JVM.
void ShutdownPlanner() {
  auto [jvm, env] = StartJVM(false);
  if (jvm != nullptr) {
    jvm->DestroyJavaVM();
  }
}

}  // namespace planner
}  // namespace pelton
