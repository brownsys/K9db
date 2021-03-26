#include <cstdint>
#include <string>

// Automatically generated via bazel dependencies from jni/PeltonJNI.java.
#include "glog/logging.h"
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
  LOG(INFO) << "Open pelton connection";
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr == 0) {
    pelton::Connection *connection = new pelton::Connection();
    pelton::open(GetString(env, directory), connection);
    env->SetLongField(this_, GetConnectionFieldID(env, this_),
                      reinterpret_cast<int64_t>(connection));
  }
}

jboolean Java_edu_brown_pelton_PeltonJNI_ExecuteDDL(JNIEnv *env, jobject this_,
                                                    jstring sql) {
  bool context = true;
  std::string str = GetString(env, sql);
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    // Execute query.
    auto callback = [&](void *context, int col_count, char **col_data,
                        char **col_name) { return 0; };
    if (pelton::exec(connection, str, callback,
                     reinterpret_cast<void *>(&context), nullptr)) {
      return JNI_TRUE;
    }
  }
  return JNI_FALSE;
}

jint Java_edu_brown_pelton_PeltonJNI_ExecuteUpdate(JNIEnv *env, jobject this_,
                                                   jstring sql) {
  bool context = true;
  std::string str = GetString(env, sql);
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    // Execute query.
    auto callback = [&](void *context, int col_count, char **col_data,
                        char **col_name) { return 0; };
    if (pelton::exec(connection, str, callback,
                     reinterpret_cast<void *>(&context), nullptr)) {
      return 1;
    }
  }
  return 0;
}

jobject Java_edu_brown_pelton_PeltonJNI_ExecuteQuery(JNIEnv *env, jobject this_,
                                                     jstring sql) {
  bool context = true;
  std::string str = GetString(env, sql);
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    // Create an ArrayList to return.
    jclass array_class = env->FindClass("java/util/ArrayList");
    jmethodID constructor_id = env->GetMethodID(array_class, "<init>", "()V");
    jobject array_list_obj = env->NewObject(array_class, constructor_id);
    // Execute query and fill in ArrayList in the callback.
    auto callback = [&](void *context, int col_count, char **col_data,
                        char **col_name) {
      jclass string_class = env->FindClass("java/lang/String");
      jclass array_class = env->FindClass("java/util/ArrayList");
      jmethodID add_id =
          env->GetMethodID(array_class, "add", "(Ljava/lang/Object;)Z");

      bool *first_time = reinterpret_cast<bool *>(context);
      // Add the column names as the first row in the result array list.
      if (*first_time) {
        *first_time = false;
        jobjectArray column_names =
            env->NewObjectArray(col_count, string_class, NULL);
        for (int i = 0; i < col_count; i++) {
          jstring column_name = env->NewStringUTF(col_name[i]);
          env->SetObjectArrayElement(column_names, i, column_name);
        }
        env->CallBooleanMethod(array_list_obj, add_id, column_names);
      }
      jobjectArray row_data =
          env->NewObjectArray(col_count, string_class, NULL);
      for (int i = 0; i < col_count; i++) {
        jstring data = env->NewStringUTF(col_data[i]);
        env->SetObjectArrayElement(row_data, i, data);
      }

      env->CallBooleanMethod(array_list_obj, add_id, row_data);
      return 0;
    };
    if (pelton::exec(connection, str, callback,
                     reinterpret_cast<void *>(&context), nullptr)) {
      return array_list_obj;
    }
  }

  return NULL;
}

void Java_edu_brown_pelton_PeltonJNI_Close(JNIEnv *env, jobject this_) {
  LOG(INFO) << "Close pelton connection";
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    pelton::close(connection);
    delete connection;
  }
  env->SetLongField(this_, GetConnectionFieldID(env, this_), 0);
}