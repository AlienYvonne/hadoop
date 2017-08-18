/**
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


#ifndef ORG_APACHEdt_HADOOP_IO_COMPRESS_ISAL_ISAL_H
#define ORG_APACHE_HADOOP_IO_COMPRESS_ISAL_ISAL_H

#include "org_apache_hadoop.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*test*/
#include <stdio.h>

#ifdef UNIX
#include <config.h>
#include <stddef.h>
#include <isa-l.h>
#include <dlfcn.h>
#include <jni.h>
#include "config.h"
#endif

#ifdef WINDOWS
#include <jni.h>
#ifndef HADOOP_ISAL_LIBRARY
#define HADOOP_ISAL_LIBRARY L"isal.dll"
#endif
#include <isa-l.h>
#include <zconf.h>
#endif


#endif //ORG_APACHE_HADOOP_IO_COMPRESS_ISAL_ISAL_H
