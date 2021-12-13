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

// JVM is created once and kept until no longer needed.
static std::atomic<JavaVM *> jvm = nullptr;

// Start a JVM embedded in this process.
void StartJVM() {
  if (jvm.load() == nullptr) {
    JavaVM *jvm_local = nullptr;
    JNIEnv *env_local = nullptr;
    // Specify the class path and library path arguments to the JVM
    // so that it can locate our calcite java package, and can load
    // the needed C++ JNI interfaces.
    JavaVMInitArgs jvm_args;

    // Finally, add all these arguments to *args.
    JavaVMOption *options = new JavaVMOption[2];
    options[0].optionString =
        static_cast<char *>(CLASS_PATH_ARG CALCITE_PATH JAR_NAME);
    options[1].optionString =
        static_cast<char *>(LIBRARY_PATH_ARG CALCITE_PATH NATIVELIB_PATH);
    // options[2].optionString = "-verbose:jni";

    jvm_args.version = JNI_VERSION_1_8;
    jvm_args.options = options;
    jvm_args.nOptions = 2;
    jvm_args.ignoreUnrecognized = false;

    // Start the JVM.
    CHECK(!JNI_CreateJavaVM(&jvm_local, reinterpret_cast<void **>(&env_local),
                            &jvm_args))
        << "failed to start JVM";
    jvm.store(jvm_local);
    delete[] options;
  }
}

}  // namespace

// Given a query, use calcite to plan its execution, and generate
// a DataFlowGraphPartition with all the operators from that plan.
std::unique_ptr<dataflow::DataFlowGraphPartition> PlanGraph(
    dataflow::DataFlowState *state, const std::string &query) {
  LOG(WARNING) << "PLANNER IS CALLED! " << query;
  perf::Start("planner");
  // Start a JVM.
  StartJVM();

  // This function could be called by different threads. Hence each for safety
  // we will attach the current thread to the JVM and get a fresh JNIEnv.
  // If the current thread is already attached to the JVM then this will be a
  // no-op.
  JNIEnv *env_local;
  JavaVMAttachArgs jvm_args;
  jvm_args.name = NULL;
  jvm_args.group = NULL;
  jvm.load()->AttachCurrentThread((void **)&env_local, &jvm_args);

  // First get the java/calcite entry point class.
  jclass QueryPlannerClass =
      env_local->FindClass("com/brownsys/pelton/QueryPlanner");

  // Get the (static) entry method from that class.
  const char *sig = "(JJLjava/lang/String;Z)V";

  jmethodID planMethod =
      env_local->GetStaticMethodID(QueryPlannerClass, "plan", sig);

  // Create the required shared state to pass to the java code.
  auto graph = std::make_unique<dataflow::DataFlowGraphPartition>(0);
  jlong graph_jptr = reinterpret_cast<jlong>(graph.get());
  jlong state_jptr = reinterpret_cast<jlong>(state);
  jstring query_jstr = env_local->NewStringUTF(query.c_str());

  // Call the method on the object
  env_local->CallStaticVoidMethod(QueryPlannerClass, planMethod, graph_jptr,
                                  state_jptr, query_jstr, PELTON_JAVA_DEBUG);
  if (env_local->ExceptionCheck()) {  // Handle any exception.
    env_local->ExceptionDescribe();
    env_local->ExceptionClear();
    LOG(FATAL) << "--- Java exception encountered during planning";
  }

  // Free up the created jstring.
  env_local->DeleteLocalRef(query_jstr);
  env_local->DeleteLocalRef(QueryPlannerClass);
  perf::End("planner");
  // Detach the current thread from the JVM.
  jvm.load()->DetachCurrentThread();

  // Return the graph.
  return graph;
}

// Shutdown the JVM.
void ShutdownPlanner() {
#ifndef PELTON_ASAN
#ifndef PELTON_TSAN
  if (jvm.load() != nullptr) {
    LOG(INFO) << "Destroying JVM...";
    jvm.load()->DestroyJavaVM();
    jvm.store(nullptr);
    LOG(INFO) << "Destroyed JVM";
  }
#endif  // PELTON_TSAN
#endif  // PELTON_ASAN
}

}  // namespace planner
}  // namespace pelton
