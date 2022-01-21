#include "java/edu_brown_pelton_MemcachedJNI.h"
#include "memcached/memcached.h"

void Java_edu_brown_pelton_MemcachedJNI_Initialize(JNIEnv *env, jclass clazz,
                                                   jstring jdb, jstring juser,
                                                   jstring jpass,
                                                   jstring jseed) {
  const char *db = env->GetStringUTFChars(jdb, NULL);
  const char *user = env->GetStringUTFChars(juser, NULL);
  const char *pass = env->GetStringUTFChars(jpass, NULL);
  const char *seed = env->GetStringUTFChars(jseed, NULL);
  Initialize(db, user, pass, seed);
  env->ReleaseStringUTFChars(jdb, db);
  env->ReleaseStringUTFChars(juser, user);
  env->ReleaseStringUTFChars(jpass, pass);
  env->ReleaseStringUTFChars(jseed, seed);
}

jint Java_edu_brown_pelton_MemcachedJNI_Cache(JNIEnv *env, jclass clazz,
                                              jstring jquery) {
  const char *query = env->GetStringUTFChars(jquery, NULL);
  int id = Cache(query);
  env->ReleaseStringUTFChars(jquery, query);
  return static_cast<jint>(id);
}

void Java_edu_brown_pelton_MemcachedJNI_Update(JNIEnv *env, jclass clazz,
                                               jstring jtable) {
  const char *table = env->GetStringUTFChars(jtable, NULL);
  Update(table);
  env->ReleaseStringUTFChars(jtable, table);
}

jobjectArray Java_edu_brown_pelton_MemcachedJNI_Read(JNIEnv *env, jclass clazz,
                                                     jint id,
                                                     jobjectArray jkey) {
  jclass JValue =
      env->FindClass("edu/brown/pelton/MemcachedJNI$MemcachedValue");
  jclass JValueArr =
      env->FindClass("[Ledu/brown/pelton/MemcachedJNI$MemcachedValue;");
  jmethodID jGetType = env->GetMethodID(JValue, "getType", "()I");
  jmethodID jGetStr =
      env->GetMethodID(JValue, "getStr", "()Ljava/lang/String;");
  jmethodID jGetInt = env->GetMethodID(JValue, "getInt", "()J");
  jmethodID jGetUInt = env->GetMethodID(JValue, "getUInt", "()J");
  jmethodID jFromStr = env->GetStaticMethodID(
      JValue, "fromStr",
      "(Ljava/lang/String;)Ledu/brown/pelton/MemcachedJNI$MemcachedValue;");
  jmethodID jFromInt = env->GetStaticMethodID(
      JValue, "fromInt", "(J)Ledu/brown/pelton/MemcachedJNI$MemcachedValue;");
  jmethodID jFromUInt = env->GetStaticMethodID(
      JValue, "fromUInt", "(J)Ledu/brown/pelton/MemcachedJNI$MemcachedValue;");

  // Transfrom key from java.
  size_t keysize = static_cast<size_t>(env->GetArrayLength(jkey));
  Record key = {keysize, new Value[keysize]};
  for (size_t i = 0; i < keysize; i++) {
    jobject jvalue = env->GetObjectArrayElement(jkey, static_cast<jsize>(i));
    int type = static_cast<int>(env->CallIntMethod(jvalue, jGetType));
    if (type == 1) {
      jstring jstr =
          static_cast<jstring>(env->CallObjectMethod(jvalue, jGetStr));
      const char *str = env->GetStringUTFChars(jstr, NULL);
      key.values[i] = SetStr(str);
      env->ReleaseStringUTFChars(jstr, str);
    } else if (type == 2) {
      jlong int64 = env->CallLongMethod(jvalue, jGetInt);
      key.values[i] = SetInt(static_cast<int64_t>(int64));
    } else if (type == 3) {
      jlong uint64 = env->CallLongMethod(jvalue, jGetUInt);
      key.values[i] = SetUInt(static_cast<uint64_t>(uint64));
    }
  }

  // Query memcached.
  Result result = Read(static_cast<int>(id), &key);
  DestroyRecord(&key);

  // Transform result to java.
  jobjectArray jresult = env->NewObjectArray(result.size, JValueArr, NULL);
  for (size_t i = 0; i < result.size; i++) {
    jobjectArray jrecord =
        env->NewObjectArray(result.records[i].size, JValue, NULL);
    for (size_t j = 0; j < result.records[i].size; j++) {
      Value &value = result.records[i].values[j];
      jobject jvalue;
      switch (value.type) {
        case TEXT: {
          jstring jstr = env->NewStringUTF(GetStr(&value));
          jvalue = env->CallStaticObjectMethod(JValue, jFromStr, jstr);
          break;
        }
        case INT: {
          int64_t int64 = GetInt(&value);
          jvalue = env->CallStaticObjectMethod(JValue, jFromInt,
                                               static_cast<jlong>(int64));
          break;
        }
        case UINT: {
          uint64_t uint64 = GetUInt(&value);
          jvalue = env->CallStaticObjectMethod(JValue, jFromUInt,
                                               static_cast<jlong>(uint64));
          break;
        }
      }
      env->SetObjectArrayElement(jrecord, j, jvalue);
    }

    env->SetObjectArrayElement(jresult, i, jrecord);
  }

  DestroyResult(&result);
  env->DeleteLocalRef(JValue);
  env->DeleteLocalRef(JValueArr);
  return jresult;
}

void Java_edu_brown_pelton_MemcachedJNI__1_1ExecuteDB(JNIEnv *env, jclass clazz,
                                                      jstring jquery) {
  const char *query = env->GetStringUTFChars(jquery, NULL);
  __ExecuteDB(query);
  env->ReleaseStringUTFChars(jquery, query);
}

void Java_edu_brown_pelton_MemcachedJNI_Close(JNIEnv *env, jclass clazz) {
  Close();
}
