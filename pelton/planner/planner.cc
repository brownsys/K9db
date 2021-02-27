#include "pelton/planner/planner.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
// NOLINTNEXTLINE
#include "jni.h"

#define MAVEN_PATH "external/maven/v1/"
#define JAVACPP_PATH \
  "https/jcenter.bintray.com/org/bytedeco/javacpp/1.5.4/javacpp-1.5.4.jar"

#define CALCITE_PATH "pelton/planner/calcite/src/com/brownsys/pelton/"
#define JAVA_MAIN "QueryPlanner.jar"
#define NATIVELIB_PATH "nativelib/"

#define CLASS_PATH_ARG "-Djava.class.path="
#define LIBRARY_PATH_ARG "-Djava.library.path="

namespace pelton {
namespace planner {

// Internal helpers for starting a JVM.
namespace {

// Specify the class path and library path arguments to the JVM
// so that it can locate our calcite java package, and can load
// the needed C++ JNI interfaces.
void InitializeJVMArgs(JavaVMInitArgs *args) {
  // Path to jars or so libraries required by the jvm.
  std::string javacpp = MAVEN_PATH JAVACPP_PATH;
  std::string javamain = CALCITE_PATH JAVA_MAIN;
  std::string generator_so = CALCITE_PATH NATIVELIB_PATH;

  // Class path and library path arguments to the JVM.
  std::string cp = CLASS_PATH_ARG + javacpp + ":" + javamain;
  std::string lp = LIBRARY_PATH_ARG + generator_so;

  // Create c-style strings that jvm understands.
  char *cp_ptr = new char[cp.size() + 1];
  char *lp_ptr = new char[lp.size() + 1];
  memcpy(cp_ptr, cp.c_str(), cp.size() + 1);
  memcpy(lp_ptr, lp.c_str(), lp.size() + 1);

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
std::pair<JavaVM *, JNIEnv *> StartJVM(bool run = true) {
  // JVM is created once and kept until no longer needed.
  static JavaVM *jvm = nullptr;
  static JNIEnv *env = nullptr;

  if (run && jvm == nullptr) {
    // Start JVM.
    JavaVMInitArgs jvm_args;
    InitializeJVMArgs(&jvm_args);
    jint res =
        JNI_CreateJavaVM(&jvm, reinterpret_cast<void **>(&env), &jvm_args);
    CHECK(res == 0) << "failed to start JVM";
  }

  // All good.
  return std::make_pair(std::move(jvm), std::move(env));
}

}  // namespace

// Given a query, use calcite to plan its execution, and generate
// a DataFlowGraph with all the operators from that plan.
dataflow::DataFlowGraph PlanGraph(dataflow::DataflowState *state,
                                  const std::string &query) {
  // Start a JVM.
  auto [jvm, env] = StartJVM();

  // First get the java/calcite entry point class.
  jclass QueryPlannerClass = env->FindClass("com/brownsys/pelton/QueryPlanner");

  // Get the (static) entry method from that class.
  const char *sig = "(JJLjava/lang/String;)V";
  jmethodID planMethod = env->GetStaticMethodID(QueryPlannerClass, "plan", sig);

  // Create the required shared state to pass to the java code.
  dataflow::DataFlowGraph graph;
  jlong graph_jptr = reinterpret_cast<jlong>(&graph);
  jlong state_jptr = reinterpret_cast<jlong>(state);
  jstring query_jstr = env->NewStringUTF(query.c_str());

  // Call the method on the object
  env->CallStaticVoidMethod(QueryPlannerClass, planMethod, graph_jptr,
                            state_jptr, query_jstr);
  if (env->ExceptionCheck()) {  // Handle any exception.
    env->ExceptionDescribe();
    env->ExceptionClear();
    LOG(FATAL) << "--- Java exception encountered during planning";
  }

  // Free up the created jstring.
  env->DeleteLocalRef(query_jstr);

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
