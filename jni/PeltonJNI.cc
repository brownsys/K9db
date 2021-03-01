#include <iostream>

// Automatically generated via bazel dependencies from jni/PeltonJNI.java.
#include "jni/edu_brown_pelton_PeltonJNI.h"

void Java_edu_brown_pelton_PeltonJNI_sayHello(JNIEnv *, jobject) {
  std::cout << "Hello from c++" << std::endl;
}
