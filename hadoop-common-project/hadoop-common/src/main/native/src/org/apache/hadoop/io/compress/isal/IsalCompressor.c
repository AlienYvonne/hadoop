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


#include "org_apache_hadoop_io_compress_isal_IsalCompressor.h"

#define JINT_MAX 0x7fffffff

static jfieldID IsalCompressor_uncompressedDirectBuf;
static jfieldID IsalCompressor_uncompressedDirectBufLen;
static jfieldID IsalCompressor_compressedDirectBuf;
static jfieldID IsalCompressor_BufferSize;
static jfieldID IsalCompressor_directBufferSize;
static jfieldID IsalCompressor_uncompressedDirectBufOff;
static jfieldID IsalCompressor_finish;
static jfieldID IsalCompressor_finished;
static jfieldID IsalCompressor_endofstream;
static jfieldID IsalCompressor_stream;
static jfieldID IsalCompressor_flush;

#ifdef UNIX  //what are these for?
static int (*dlsym_isal_deflate_init)(struct isal_zstream*);
static int (*dlsym_isal_deflate)(struct isal_zstream*);



#endif

#ifdef WINDOWS
typedef int (__cdecl *__dlsym_isal_deflate)(struct isal_zstream*);
static __dlsym_isal_deflate dlsym_isal_deflate;

typedef int (__cdecl *__dlsym_isal_deflate_init)(struct isal_zstream*);
static __dlsym_isal_deflate_init dlsym_isal_deflate_init;
#endif

int LEVEL;
int FLUSH;
int GZIP_FLAG;


JNIEXPORT void JNICALL Java_org_apache_hadoop_io_compress_isal_IsalCompressor_initIDs
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
  LOAD_DYNAMIC_SYMBOL(dlsym_isal_deflate, env, libisal, "isal_deflate");
  LOAD_DYNAMIC_SYMBOL(dlsym_isal_deflate_init, env, libisal, "isal_deflate_init");
#endif

#ifdef WINDOWS
  LOAD_DYNAMIC_SYMBOL(__dlsym_isal_deflate, dlsym_isal_deflate, env, libisal, "isal_deflate");
  LOAD_DYNAMIC_SYMBOL(__dlsym_isal_deflate_init, dlsym_isal_deflate_init, env, libisal, "isal_deflate_init");
#endif


  /*if (!stream) {
  		THROW(env, "java/lang/OutOfMemoryError", NULL);
  		return (jlong)0;
        }*/

  IsalCompressor_uncompressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                           "uncompressedDirectBuf",
                                                           "Ljava/nio/Buffer;");
  IsalCompressor_uncompressedDirectBufLen = (*env)->GetFieldID(env, clazz,
                                                              "uncompressedDirectBufLen", "I");

  IsalCompressor_uncompressedDirectBufOff = (*env)->GetFieldID(env,clazz,
                                                        "uncompressedDirectBufOff","I");

  IsalCompressor_compressedDirectBuf = (*env)->GetFieldID(env, clazz,
                                                           "compressedDirectBuf",
                                                           "Ljava/nio/Buffer;");

  IsalCompressor_directBufferSize = (*env)->GetFieldID(env, clazz,
                                                       "directBufferSize", "I");

  IsalCompressor_BufferSize = (*env)->GetFieldID(env,clazz,"BufferSize","I");


  IsalCompressor_finish = (*env)->GetFieldID(env,clazz,"finish","Z");
  IsalCompressor_finished = (*env)->GetFieldID(env,clazz,"finished","Z");

  IsalCompressor_endofstream = (*env)->GetFieldID(env,clazz,"end_of_stream","I");
  IsalCompressor_stream = (*env)->GetFieldID(env,clazz,"stream","J");

  //IsalCompressor_flush = (*env)->GetFieldID(env,clazz,"flush","I");
}


struct isal_zstream * ZSTREAM_DEFLATE(long stream)
{
    return (struct isal_zstream*)(stream);
}

long JLONG_INFLATE(struct isal_zstream * stream)
{
    return (long)(stream);
}

JNIEXPORT jlong JNICALL Java_org_apache_hadoop_io_compress_isal_IsalCompressor_init
  (JNIEnv *env, jclass thisj, jint level, jint flush,jint gzip_flag){
  LEVEL = level;
  FLUSH = flush;
  GZIP_FLAG = gzip_flag;



  struct isal_zstream *stream = malloc(sizeof(struct isal_zstream));
  if(!stream){
  	THROW(env, "java/lang/OutOfMemoryError", NULL);
  	return (jlong)0;
  }

  memset((void *)stream,0,sizeof(struct isal_zstream));

  dlsym_isal_deflate_init(stream);

    if (LEVEL == 1) {
      stream->level = 1;
      stream->level_buf = malloc(ISAL_DEF_LVL1_DEFAULT);
      stream->level_buf_size = ISAL_DEF_LVL1_DEFAULT;
      if (stream->level_buf == 0) {
        printf("Failed to allocate level compression buffer\n");
        exit(0);
      }
    }

  stream->gzip_flag = GZIP_FLAG;
  stream->level = LEVEL;
  stream->flush = FLUSH;//FULL_FLUSH:NO_FLUSH;

  return JLONG_INFLATE(stream);
  }


JNIEXPORT jint JNICALL Java_org_apache_hadoop_io_compress_isal_IsalCompressor_compressBytesDirect
(JNIEnv *env, jobject thisj){

  //struct isal_zstream *stream = malloc(sizeof(struct isal_zstream));
  int rv = 0;
  uint8_t* uncompressed_bytes = NULL;
  uint8_t* compressed_bytes = NULL;
  struct isal_zstream *stream = ZSTREAM_DEFLATE((*env)->GetLongField(env, thisj,
                                            		IsalCompressor_stream));
  if (!stream) {
  		THROW(env, "java/lang/NullPointerException", NULL);
  		return (jint)0;
  }


  // Get members of IsalCompressor
  jarray uncompressed_direct_buf = (jarray)(*env)->GetObjectField(env, thisj, IsalCompressor_uncompressedDirectBuf);
  jint uncompressed_direct_buf_len = (*env)->GetIntField(env, thisj, IsalCompressor_uncompressedDirectBufLen);
  jint uncompressed_direct_buf_off = (*env)->GetIntField(env, thisj,IsalCompressor_uncompressedDirectBufOff);
  jarray compressed_direct_buf = (jarray)(*env)->GetObjectField(env, thisj, IsalCompressor_compressedDirectBuf);
  jint compressed_direct_buf_len = (*env)->GetIntField(env, thisj, IsalCompressor_directBufferSize);
  jint compressed_buf_len = (*env)->GetIntField(env,thisj,IsalCompressor_BufferSize);
  jint no_compressed_bytes = 0;
  jboolean finish = (*env)->GetBooleanField(env,thisj,IsalCompressor_finish);
  jint end_of_stream = (*env)->GetIntField(env,thisj,IsalCompressor_endofstream);
  size_t buf_len = 0;

  compressed_bytes = (*env)->GetDirectBufferAddress(env,
										compressed_direct_buf);
  uncompressed_bytes = (*env)->GetDirectBufferAddress(env,
											uncompressed_direct_buf);

  if (uncompressed_bytes == 0) {
    return (jint)0;
  }

  if (compressed_bytes == 0) {
    return (jint)0;
  }

  // Re-calibrate the isal_zstream
  stream->avail_in = uncompressed_direct_buf_len;
  stream->end_of_stream = end_of_stream;//1; // //!< non-zero if this is the last input buffer
  stream->next_in = uncompressed_bytes + uncompressed_direct_buf_off;
  stream->avail_out = compressed_buf_len;
  stream->next_out = compressed_bytes;

  // compress
  rv = dlsym_isal_deflate(stream);



  switch (rv){
    case COMP_OK:{

        uncompressed_direct_buf_off += uncompressed_direct_buf_len - stream->avail_in;
        (*env)->SetIntField(env, thisj,
							IsalCompressor_uncompressedDirectBufOff, uncompressed_direct_buf_off);
        (*env)->SetIntField(env, thisj,
							IsalCompressor_uncompressedDirectBufLen, stream->avail_in);
        no_compressed_bytes = compressed_buf_len - stream->avail_out;

    }
    break;
    case INVALID_FLUSH:{

        THROW(env, "java/lang/IllegalArgumentException", NULL);
    }
    break;
    case ISAL_INVALID_LEVEL:{

        THROW(env, "java/lang/IllegalArgumentException", NULL);
    }
    break;
  }

  if(stream->internal_state.state == ZSTATE_END ){
        (*env)->SetBooleanField(env, thisj, IsalCompressor_finished, JNI_TRUE);
    }

  return (jint)no_compressed_bytes;

}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalCompressor_getBytesRead(
	JNIEnv *env, jclass class, jlong stream
	) {
    return (ZSTREAM_DEFLATE(stream))->total_in;
}

JNIEXPORT jlong JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalCompressor_getBytesWritten(
	JNIEnv *env, jclass class, jlong stream
	) {
    return (ZSTREAM_DEFLATE(stream))->total_out;
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalCompressor_reset(
	JNIEnv *env, jclass class, jlong stream
	) {
	dlsym_isal_deflate_init(ZSTREAM_DEFLATE(stream));

}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalCompressor_end(
	JNIEnv *env, jclass class, jlong stream
	) {
    free(ZSTREAM_DEFLATE(stream));
}



JNIEXPORT jstring JNICALL
Java_org_apache_hadoop_io_compress_isal_IsalCompressor_getLibraryName(JNIEnv *env, jclass class) {
#ifdef UNIX
  if (dlsym_isal_deflate) {
    Dl_info dl_info;
    if(dladdr(
        dlsym_isal_deflate,
        &dl_info)) {
      return (*env)->NewStringUTF(env, dl_info.dli_fname);
    }
  }

  return (*env)->NewStringUTF(env, HADOOP_ISAL_LIBRARY);
#endif

#ifdef WINDOWS
  LPWSTR filename = NULL;
  GetLibraryName(dlsym_isal_deflate, &filename);
  if (filename != NULL) {
    return (*env)->NewString(env, filename, (jsize) wcslen(filename));
  } else {
    return (*env)->NewStringUTF(env, "Unavailable");
  }
#endif
}
#endif //define HADOOP_ISAL_LIBRARY
