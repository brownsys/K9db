#include <cstdint>
#include <memory>
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
                                          jstring directory, jstring db_name,
                                          jstring username, jstring password) {
  LOG(INFO) << "Open pelton connection";
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr == 0) {
    pelton::Connection *connection = new pelton::Connection();
    pelton::initialize(GetString(env, directory));
    pelton::open(connection, GetString(env, db_name), GetString(env, username),
                 GetString(env, password));
    env->SetLongField(this_, GetConnectionFieldID(env, this_),
                      reinterpret_cast<int64_t>(connection));
  }
}

jboolean Java_edu_brown_pelton_PeltonJNI_ExecuteDDL(JNIEnv *env, jobject this_,
                                                    jstring sql) {
  std::string str = GetString(env, sql);
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    absl::StatusOr<pelton::SqlResult> result = pelton::exec(connection, str);
    if (result.ok() && result.value().IsStatement() &&
        result.value().Success()) {
      return JNI_TRUE;
    }
  }
  return JNI_FALSE;
}

jint Java_edu_brown_pelton_PeltonJNI_ExecuteUpdate(JNIEnv *env, jobject this_,
                                                   jstring sql) {
  std::string str = GetString(env, sql);
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    absl::StatusOr<pelton::SqlResult> result = pelton::exec(connection, str);
    if (result.ok() && result.value().IsUpdate()) {
      return result.value().UpdateCount();
    }
  }
  return -1;
}

jobject Java_edu_brown_pelton_PeltonJNI_ExecuteQuery(JNIEnv *env, jobject this_,
                                                     jstring sql) {
  std::string str = GetString(env, sql);
  int64_t ptr = env->GetLongField(this_, GetConnectionFieldID(env, this_));
  if (ptr != 0) {
    pelton::Connection *connection =
        reinterpret_cast<pelton::Connection *>(ptr);
    // Create an ArrayList to return.
    jclass string_class = env->FindClass("java/lang/String");
    jclass array_class = env->FindClass("java/util/ArrayList");
    jmethodID add_id =
        env->GetMethodID(array_class, "add", "(Ljava/lang/Object;)Z");
    jmethodID constructor_id = env->GetMethodID(array_class, "<init>", "()V");
    jobject array_list_obj = env->NewObject(array_class, constructor_id);
    // Execute query with pelton.
    absl::StatusOr<pelton::SqlResult> result = pelton::exec(connection, str);
    if (result.ok() && result.value().IsQuery()) {
      pelton::SqlResult &sqlresult = result.value();
      std::unique_ptr<pelton::SqlResultSet> resultset =
          sqlresult.NextResultSet();
      size_t col_count = resultset->GetSchema().size();
      // First element in result contains column names / headers.
      jobjectArray column_names =
          env->NewObjectArray(col_count, string_class, NULL);
      for (size_t i = 0; i < col_count; i++) {
        jstring column_name =
            env->NewStringUTF(resultset->GetSchema().NameOf(i).c_str());
        env->SetObjectArrayElement(column_names, i, column_name);
      }
      env->CallBooleanMethod(array_list_obj, add_id, column_names);

      // Remaining elements contain actual rows.
      while (resultset->HasNext()) {
        pelton::Record row = resultset->FetchOne();
        jobjectArray row_data =
            env->NewObjectArray(col_count, string_class, NULL);
        for (size_t i = 0; i < col_count; i++) {
          jstring data = env->NewStringUTF(row.GetValueString(i).c_str());
          env->SetObjectArrayElement(row_data, i, data);
        }
        env->CallBooleanMethod(array_list_obj, add_id, row_data);
      }
      // All data added.
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
    pelton::shutdown();
    delete connection;
  }
  env->SetLongField(this_, GetConnectionFieldID(env, this_), 0);
}
