#include "k9db/planner/planner.h"

#include <cstring>
#include <utility>

#include "glog/logging.h"
// NOLINTNEXTLINE
#include "jni.h"

#define CALCITE_PATH "k9db/planner/calcite/src/com/brownsys/k9db/"
#define JAR_NAME "QueryPlanner_deploy.jar"
#define NATIVELIB_PATH "nativelib/"

#define CLASS_PATH_ARG "-Djava.class.path="
#define LIBRARY_PATH_ARG "-Djava.library.path="

// Configure java logging level according to compilation mode/config.
#ifndef K9DB_OPT
#define K9DB_JAVA_DEBUG true
#else
#define K9DB_JAVA_DEBUG false
#endif  // K9DB_OPT

namespace k9db {
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
  LOG(WARNING) << "Functionality Warning: PLANNER IS CALLED! " << query;
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
  CHECK_EQ(jvm.load()->AttachCurrentThread(
               reinterpret_cast<void **>(&env_local), &jvm_args),
           JNI_OK);

  // First get the java/calcite entry point class.
  jclass QueryPlannerClass =
      env_local->FindClass("com/brownsys/k9db/QueryPlanner");

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
                                  state_jptr, query_jstr, K9DB_JAVA_DEBUG);
  if (env_local->ExceptionCheck()) {  // Handle any exception.
    env_local->ExceptionDescribe();
    env_local->ExceptionClear();
    LOG(FATAL) << "--- Java exception encountered during planning";
  }

  // Free up the created jstring.
  env_local->DeleteLocalRef(query_jstr);
  env_local->DeleteLocalRef(QueryPlannerClass);
  // Detach the current thread from the JVM.
  CHECK_EQ(jvm.load()->DetachCurrentThread(), JNI_OK);

  // Return the graph.
  return graph;
}

// Shutdown the JVM.
void ShutdownPlanner(bool shutdown_jvm) {
#ifndef K9DB_ASAN
#ifndef K9DB_TSAN
  if (shutdown_jvm && jvm.load() != nullptr) {
    LOG(INFO) << "Destroying JVM...";
    if (!jvm.load()->DestroyJavaVM()) {
      LOG(WARNING)
          << "Unloading the jvm is not supported (resources not reclaimed).";
    } else {
      jvm.store(nullptr);
      LOG(INFO) << "Destroyed JVM";
    }
  }
#endif  // K9DB_TSAN
#endif  // K9DB_ASAN
}

void ShutdownPlanner() { ShutdownPlanner(true); }

}  // namespace planner
}  // namespace k9db
