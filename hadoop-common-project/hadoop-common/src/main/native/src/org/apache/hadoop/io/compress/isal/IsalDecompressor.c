/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "org_apache_hadoop_io_compress_isal.h"

#if defined HADOOP_ISAL_LIBRARY

#include "org_apache_hadoop_io_compress_isal_IsalDecompressor.h"

#define JINT_MAX 0x7fffffff

static jfieldID IsalDecompressor_compressedDirectBuf;
static jfieldID IsalDecompressor_compressedDirectBufLen;
static jfieldID IsalDecompressor_compressedDirectBufOff;
static jfieldID IsalDecompressor_uncompressedDirectBuf;
static jfieldID IsalDecompressor_directBufferSize;
static jfieldID IsalDecompressor_finished;
static jfieldID IsalDecompressor_stream;

int GZIP_FLAG;

#ifdef UNIX
static int (*dlsym_isal_inflate_init)(struct inflate_state*);
static int (*dlsym_isal_inflate)(struct inflate_state*);

#endif

#ifdef WINDOWS
typedef int (__cdecl *__dlsym_isal_inflate)(struct inflate_state*);
typedef int (__cdecl *__dlsym_isal_inflate_init)(struct inflate_state*);
static __dlsym_isal_inflate_init dlsym_isal_inflate;

static __dlsym_isal_inflate_init dlsym_isal_inflate_init;
#endif

JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_initIDs
(JNIEnv *env, jclass clazz){
#ifdef UNIX
  // Load libisal.so
  void *libisal = dlopen(HADOOP_ISAL_LIBRARY, RTLD_LAZY | RTLD_GLOBAL);
  if (!libisal) {
    char msg[1000];
    snprintf(msg, 1000, "%s (%s)!", "Cannot load " HADOOP_ISAL_LIBRARY, dlerror());
    THROW(env, "java/lang/UnsatisfiedLinkError", msg);
    return;
  }
#endif

#ifdef WINDOWS
  HMODULE libisal = LoadLibrary(HADOOP_ISAL_LIBRARY);
  if (!libisal) {
    THROW(env, "java/lang/UnsatisfiedLinkError", "Cannot load isal.dll");
    return;
  }
#endif

  // Locate the requisite symbols from libisal.so
#ifdef UNIX
  dlerror();                                 // Clear any existing error
  LOAD_DYNAMIC_SYMBOL(dlsym_isal_inflate, env, libisal, "isal_inflate");
  LOAD_DYNAMIC_SYMBOL(dlsym_isal_inflate_init, env, libisal, "isal_inflate_init");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_isal_inflate, dlsym_isal_inflate, env, libisal, "isal_inflate");
  LOAD_DYNAMIC_SYMBOL(__dlsym_isal_inflate_init, dlsym_isal_inflate_init, env, libisal, "isal_inflate_init");
#endif


  IsalDecompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                           "uncompressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  IsalDecompressor_compressedDirectBufLen = (*env)->GetFieldID(env, clazz,
                                                              "compressedDirectBufLen", "I");
  IsalDecompressor_compressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                         "compressedDirectBuf",
                                                         "Ljava/nio/Buffer;");
  IsalDecompressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                       "directBufferSize", "I");
  IsalDecompressor_compressedDirectBufOff = (*env)->GetFieldID(env,clazz,
                                                        "compressedDirectBufOff","I");
  IsalDecompressor_finished = (*env)->GetFieldID(env,clazz,"finished","Z");

  IsalDecompressor_stream = (*env)->GetFieldID(env,clazz,"stream","J");
}

struct inflate_state * ZSTREAM(long stream)
{
    return (struct inflate_state *)(stream);
}

long JLONG(struct inflate_state * stream)
{
    return (long)(stream);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_init
  (JNIEnv *env, jclass thisj, jint gzip_flag){
  GZIP_FLAG = gzip_flag;
  struct inflate_state *state = malloc(sizeof(struct inflate_state));
  if(state == 0){
    THROW(env, "java/lang/OutOfMemoryError", NULL);
    return (jlong)0;
  }
  dlsym_isal_inflate_init(state);

  memset((void*)state,0,sizeof(struct inflate_state));

  return JLONG(state);
  }




JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_inflateBytesDirect
(JNIEnv *env, jobject thisj){

  //struct inflate_state *state = malloc(sizeof(struct inflate_state));
  int rv = 0;
  uint8_t* uncompressed_bytes = NULL;
  uint8_t* compressed_bytes = NULL;
  int no_decompressed_bytes = 0;
  struct inflate_state *state = ZSTREAM((*env)->GetLongField(env,thisj,IsalDecompressor_stream));

  if(!state){
    THROW(env, "java/lang/NullPointerException", NULL);
    return (jint)0;
  }


  // Get members of IsalDecompressor
  jarray uncompressed_direct_buf = (jarray)(*env)->GetObjectField(env, thisj, IsalDecompressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env,thisj,IsalDecompressor_directBufferSize);

  jarray compressed_direct_buf = (jarray)(*env)->GetObjectField(env, thisj, IsalDecompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, IsalDecompressor_compressedDirectBufLen);
  jint compressed_direct_buf_off = (*env)->GetIntField(env,thisj,IsalDecompressor_compressedDirectBufOff);

  // Get the input direct buffer
  compressed_bytes = (*env)->GetDirectBufferAddress(env, compressed_direct_buf);
  uncompressed_bytes = (*env)->GetDirectBufferAddress(env, uncompressed_direct_buf);
  if (!compressed_bytes) {
    return (jint)0;
  }

  // Get the output direct Buffer

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  // Re-calibrate the inflate_state
  state->avail_in = compressed_direct_buf_len;
  state->next_in = compressed_bytes + compressed_direct_buf_off;
  state->avail_out = uncompressed_direct_buf_len;
  state->next_out = uncompressed_bytes;

  // decompress
  rv = dlsym_isal_inflate(state);

  if(state->block_state == ISAL_BLOCK_FINISH){
    (*env)->SetBooleanField(env, thisj, IsalDecompressor_finished, JNI_TRUE);
  }

   switch (rv){
    case ISAL_END_INPUT:
    {
        //(*env)->SetBooleanField(env, thisj, IsalDecompressor_finished, JNI_TRUE);
    }
    case ISAL_DECOMP_OK:
    {
         compressed_direct_buf_off += compressed_direct_buf_len - state->avail_in;
         (*env)->SetIntField(env, thisj, IsalDecompressor_compressedDirectBufOff,
					compressed_direct_buf_off);
	     (*env)->SetIntField(env, thisj, IsalDecompressor_compressedDirectBufLen,
					state->avail_in);
	     no_decompressed_bytes = uncompressed_direct_buf_len - state->avail_out;

    }
    break;
    case ISAL_OUT_OVERFLOW:
    {
        THROW(env, "java/io/IOException", "ISAL_OUT_OVERFLOW");
    }
    break;
    case ISAL_INVALID_BLOCK:
    {
        THROW(env, "java/io/IOException", "ISAL_INVALID_BLOCK");
    }
    break;
    case ISAL_INVALID_SYMBOL:
    {

        THROW(env, "java/io/IOException", "ISAL_INVALID_SYMBOL");
    }
    break;
    case ISAL_INVALID_LOOKBACK:
    {
        THROW(env, "java/io/IOException", "ISAL_INVALID_LOOKBACK");
    }
    break;
    }

  return (jint)no_decompressed_bytes;
}


/*
JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_zlib_IsalDecompressor_getBytesRead(
	JNIEnv *env, jclass cls, jlong stream
	) {
    return (ZSTREAM(stream))->total_in;
}*/
/* strcut inflate_state has no variable named "total_in" */

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_getBytesWritten(
	JNIEnv *env, jclass cls, jlong stream
	) {
    return (ZSTREAM(stream))->total_out;
}

JNIEXPORT jint JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_getRemaining(
	JNIEnv *env, jclass cls, jlong stream
	) {
    return (ZSTREAM(stream))->avail_in;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_reset(
	JNIEnv *env, jclass cls, jlong stream
	) {
	dlsym_isal_inflate_init(ZSTREAM(stream));

}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_end(
	JNIEnv *env, jclass cls, jlong stream
	) {

	free(ZSTREAM(stream));

}




JNIEXPORT jstring JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalDecompressor_getLibraryName(JNIEnv *env, jclass class) {
#ifdef UNIX
  if (dlsym_isal_inflate) {
    Dl_info dl_info;
    if(dladdr(
        dlsym_isal_inflate,
        &dl_info)) {
      return (*env)->NewStringUTF(env, dl_info.dli_fname);
    }
  }

  return (*env)->NewStringUTF(env, HADOOP_ISAL_LIBRARY);
#endif

#ifdef WINDOWS
  LPWSTR filename = NULL;
  GetLibraryName(dlsym_isal_compress, &filename);
  if (filename != NULL) {
    return (*env)->NewString(env, filename, (jsize) wcslen(filename));
  } else {
    return (*env)->NewStringUTF(env, "Unavailable");
  }
#endif
}
#endif //define HADOOP_ISAL_LIBRARY
