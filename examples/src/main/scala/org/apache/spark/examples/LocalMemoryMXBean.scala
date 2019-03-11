/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.lang.management.{ManagementFactory, MemoryMXBean}

// scalastyle:off println
package org.apache.spark.examples

object LocalMemoryMXBean {
  def main(args: Array[String]) {
    val memoryMXBean: MemoryMXBean = ManagementFactory.getMemoryMXBean()
    println("Heap Memory Usage: " + memoryMXBean.getHeapMemoryUsage)

    var arr = new Array(10000)
    var list = List.fill(1024)(10)
    arr = null
    list = null
    println("Heap Memory Usage: " + memoryMXBean.getHeapMemoryUsage)

    System.gc()
    println("Heap Memory Usage: " + memoryMXBean.getHeapMemoryUsage)
  }
}
// scalastyle:on println
