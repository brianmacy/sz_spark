/*
 * JNI shim for record-transform plugins (C ABI version 1, see sz_queue_combined_consumer crates/transform-abi/src/lib.rs).
 * Java side: com.senzing.spark.transform.SzRtNative.
 *
 * Ownership: every char* the plugin returns (output record, error message) is plugin-allocated and released ONLY with
 * sz_rt_free. The input record is copied out of the Java array into a native buffer first, so no JNI critical section is held
 * while plugin code runs (the plugin may run for milliseconds; a held critical section would block the garbage collector).
 * The library is never dlclose'd: the plugin keeps threads/static state (onnxruntime) that must not be unmapped under it.
 * Thread safety: a handle is immutable after open(); transform() is reentrant (the plugin ABI requires thread safety).
 */
#include <dlfcn.h>
#include <jni.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SZ_RT_ABI_VERSION 1u
#define SZ_RT_UNCHANGED 0
#define SZ_RT_REPLACED 1

typedef uint32_t (*abi_version_fn)(void);
typedef void *(*create_fn)(const char *, char **);
typedef int32_t (*transform_fn)(void *, const char *, size_t, char **, size_t *, char **);
typedef void (*free_fn)(char *);
typedef void (*destroy_fn)(void *);

typedef struct {
  void *lib;
  void *instance;
  transform_fn transform;
  free_fn free;
  destroy_fn destroy;
} szrt_handle;

static void throw_szrt(JNIEnv *env, const char *message) {
  jclass cls = (*env)->FindClass(env, "com/senzing/spark/transform/SzRtException");
  if (cls != NULL) (*env)->ThrowNew(env, cls, message);
}

/* Message from the plugin (freed with its sz_rt_free) or a fallback when it gave none. */
static void throw_plugin_error(JNIEnv *env, free_fn pfree, char *err, const char *fallback) {
  if (err != NULL) {
    throw_szrt(env, err);
    if (pfree != NULL) pfree(err);
  } else {
    throw_szrt(env, fallback);
  }
}

static int resolve(JNIEnv *env, void *lib, const char *name, void **out) {
  *out = dlsym(lib, name);
  if (*out == NULL) {
    char msg[512];
    snprintf(msg, sizeof msg, "record-transform plugin lacks symbol %s", name);
    throw_szrt(env, msg);
    return 0;
  }
  return 1;
}

JNIEXPORT jlong JNICALL Java_com_senzing_spark_transform_SzRtNative_open(JNIEnv *env, jclass cls, jstring jpath, jstring jconfig) {
  (void)cls;
  const char *path = (*env)->GetStringUTFChars(env, jpath, NULL);
  const char *config = (*env)->GetStringUTFChars(env, jconfig, NULL);
  szrt_handle *h = NULL;
  void *lib = NULL;
  jlong result = 0;
  if (path == NULL || config == NULL) goto done; /* OutOfMemoryError already pending */

  lib = dlopen(path, RTLD_NOW | RTLD_LOCAL);
  if (lib == NULL) {
    char msg[1024];
    snprintf(msg, sizeof msg, "cannot dlopen record-transform plugin %s: %s", path, dlerror());
    throw_szrt(env, msg);
    goto done;
  }
  void *p_version, *p_create, *p_transform, *p_free, *p_destroy;
  if (!resolve(env, lib, "sz_rt_abi_version", &p_version) || !resolve(env, lib, "sz_rt_create", &p_create) ||
      !resolve(env, lib, "sz_rt_transform", &p_transform) || !resolve(env, lib, "sz_rt_free", &p_free) ||
      !resolve(env, lib, "sz_rt_destroy", &p_destroy))
    goto done;
  uint32_t version = ((abi_version_fn)p_version)();
  if (version != SZ_RT_ABI_VERSION) {
    char msg[256];
    snprintf(msg, sizeof msg, "record-transform plugin ABI version %u, this build speaks %u", version, SZ_RT_ABI_VERSION);
    throw_szrt(env, msg);
    goto done;
  }
  char *err = NULL;
  void *instance = ((create_fn)p_create)(config, &err);
  if (instance == NULL) {
    throw_plugin_error(env, (free_fn)p_free, err, "sz_rt_create failed without a message");
    goto done;
  }
  h = (szrt_handle *)calloc(1, sizeof *h);
  if (h == NULL) {
    ((destroy_fn)p_destroy)(instance);
    throw_szrt(env, "out of memory");
    goto done;
  }
  h->lib = lib;
  h->instance = instance;
  h->transform = (transform_fn)p_transform;
  h->free = (free_fn)p_free;
  h->destroy = (destroy_fn)p_destroy;
  result = (jlong)(intptr_t)h;
done:
  if (path != NULL) (*env)->ReleaseStringUTFChars(env, jpath, path);
  if (config != NULL) (*env)->ReleaseStringUTFChars(env, jconfig, config);
  return result;
}

JNIEXPORT jbyteArray JNICALL Java_com_senzing_spark_transform_SzRtNative_transform(JNIEnv *env, jclass cls, jlong handle, jbyteArray record) {
  (void)cls;
  szrt_handle *h = (szrt_handle *)(intptr_t)handle;
  jsize n = (*env)->GetArrayLength(env, record);
  char *in = (char *)malloc(n > 0 ? (size_t)n : 1);
  if (in == NULL) {
    throw_szrt(env, "out of memory");
    return NULL;
  }
  (*env)->GetByteArrayRegion(env, record, 0, n, (jbyte *)in);
  if ((*env)->ExceptionCheck(env)) {
    free(in);
    return NULL;
  }
  char *out = NULL, *err = NULL;
  size_t out_len = 0;
  int32_t rc = h->transform(h->instance, in, (size_t)n, &out, &out_len, &err);
  free(in);
  jbyteArray result = NULL;
  if (rc == SZ_RT_UNCHANGED) {
    /* *out untouched by contract */
  } else if (rc == SZ_RT_REPLACED) {
    if (out == NULL || out_len > 0x7fffffffu) {
      throw_szrt(env, "plugin returned REPLACED without a usable output buffer");
    } else {
      result = (*env)->NewByteArray(env, (jsize)out_len);
      if (result != NULL) (*env)->SetByteArrayRegion(env, result, 0, (jsize)out_len, (const jbyte *)out);
    }
    if (out != NULL) h->free(out);
  } else {
    throw_plugin_error(env, h->free, err, "plugin rejected the record without a message");
  }
  return result;
}

JNIEXPORT void JNICALL Java_com_senzing_spark_transform_SzRtNative_close(JNIEnv *env, jclass cls, jlong handle) {
  (void)env;
  (void)cls;
  szrt_handle *h = (szrt_handle *)(intptr_t)handle;
  if (h == NULL) return;
  h->destroy(h->instance);
  free(h); /* the library stays mapped on purpose */
}
