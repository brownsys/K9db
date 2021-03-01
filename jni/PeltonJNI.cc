#include <sqlite3.h>

#include <iostream>
#include <string>

// Automatically generated via bazel dependencies from jni/PeltonJNI.java.
#include "jni/edu_brown_pelton_PeltonJNI.h"
#include "pelton/pelton.h"

namespace {

// Get pointer field straight from `JavaClass`
inline jfieldID GetConnectionFieldID(JNIEnv *env, jobject obj) {
  jclass c = env->GetObjectClass(obj);
  jfieldID connection_field_id = env->GetFieldID(c, "connection", "J");
  env->DeleteLocalRef(c);
  return connection_field_id;
}

inline std::string GetString(JNIEnv *env, jstring string) {
  const char *buff = env->GetStringUTFChars(string, NULL);
  std::string result(buff, env->GetStringUTFLength(string));
  env->ReleaseStringUTFChars(string, buff);
  return result;
}

}  // namespace

void Java_edu_brown_pelton_PeltonJNI_Open(JNIEnv *env, jobject this_,
                                          jstring directory) {
  std::cout << "Open pelton connection" << std::endl;
  long ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr == 0) {
    pelton::Connection *connection = new pelton::Connection();
    pelton::open(GetString(env, directory), connection);
    env->SetLongField(this_, GetConnectionFieldID(env, this_),
                      reinterpret_cast<long>(connection));
  }
}

jboolean Java_edu_brown_pelton_PeltonJNI_ExecuteDDL(JNIEnv *env, jobject this_,
                                                    jstring sql) {
  std::string str = GetString(env, sql);
  std::cout << "DDL: " << str << std::endl;
  return JNI_FALSE;
}

jint Java_edu_brown_pelton_PeltonJNI_ExecuteUpdate(JNIEnv *env, jobject this_,
                                                   jstring sql) {
  std::string str = GetString(env, sql);
  std::cout << "DML: " << str << std::endl;
  return 0;
}

jobjectArray Java_edu_brown_pelton_PeltonJNI_ExecuteQuery(JNIEnv *env,
                                                          jobject this_,
                                                          jstring sql) {
  std::string str = GetString(env, sql);
  std::cout << "Query: " << str << std::endl;
  return NULL;
}

void Java_edu_brown_pelton_PeltonJNI_Close(JNIEnv *env, jobject this_) {
  std::cout << "Close pelton connection" << std::endl;
  long ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    pelton::close(connection);
    delete connection;
  }
  env->SetLongField(this_, GetConnectionFieldID(env, this_), 0);
}
