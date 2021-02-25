#include <jni.h>

#include <cstring>
#include <iostream>
#include <memory>

#include "pelton/dataflow/graph.h"
#include "pelton/dataflow/ops/input.h"
#include "pelton/dataflow/schema.h"
#include "pelton/dataflow/state.h"
#include "pelton/dataflow/types.h"

int main(int argc, char **argv) {
  pelton::dataflow::DataFlowGraph graph;
  uint64_t graph_ptr = reinterpret_cast<uint64_t>(&graph);
  pelton::dataflow::DataflowState state;
  uint64_t state_ptr = reinterpret_cast<uint64_t>(&state);
  state.AddTableSchema("hello-world-table",
                       pelton::dataflow::SchemaOwner({}, {}, {}));

  size_t len = strlen(argv[0]);
  std::string dir(argv[0], len - 8);

  std::string classpath = "-Djava.class.path=";
  classpath += dir;
  classpath +=
      "/wrapper.runfiles/maven/v1/https/jcenter.bintray.com/org/bytedeco/"
      "javacpp/1.5.4/javacpp-1.5.4.jar:";
  classpath += dir;
  classpath +=
      "/wrapper.runfiles/pelton/pelton/planner/calcite/src/main/java/com/"
      "brownsys/pelton/planner/nativelib/Main.jar";

  std::string librarypath = "-Djava.library.path=";
  librarypath += dir;
  librarypath +=
      "/wrapper.runfiles/pelton/pelton/planner/calcite/src/main/java/com/"
      "brownsys/pelton/planner/nativelib";

  char *cp = new char[classpath.size() + 1];
  memcpy(cp, classpath.c_str(), classpath.size() + 1);

  char *lp = new char[librarypath.size() + 1];
  memcpy(lp, librarypath.c_str(), librarypath.size() + 1);

  JavaVM *vm;
  JNIEnv *env;

  JavaVMInitArgs vm_args;
  JavaVMOption options[2];
  options[0].optionString = cp;
  options[1].optionString = lp;
  vm_args.version = JNI_VERSION_10;
  vm_args.nOptions = 2;
  vm_args.options = options;
  vm_args.ignoreUnrecognized = JNI_TRUE;
  JNI_GetDefaultJavaVMInitArgs(&vm_args);

  // Construct a VM
  jint res = JNI_CreateJavaVM(&vm, reinterpret_cast<void **>(&env), &vm_args);

  // First get the class that contains the method you need to call
  jclass clazz = env->FindClass("com/brownsys/pelton/planner/nativelib/Main");

  // Get the method that you want to call
  jmethodID main = env->GetStaticMethodID(clazz, "main", "(JJ)V");

  // Call the method on the object
  env->CallStaticVoidMethod(clazz, main, static_cast<jlong>(graph_ptr),
                            static_cast<jlong>(state_ptr));
  if (env->ExceptionCheck()) {
    env->ExceptionDescribe();
    env->ExceptionClear();
  }
  std::cout << graph.inputs().at("hello-world-table")->input_name() << std::endl;
  std::cout << std::static_pointer_cast<pelton::dataflow::InputOperator>(
                   graph.GetNode(0))
                   ->input_name()
            << std::endl;

  // Shutdown the VM.
  vm->DestroyJavaVM();
}
