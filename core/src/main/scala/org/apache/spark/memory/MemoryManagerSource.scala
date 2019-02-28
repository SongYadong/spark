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

package org.apache.spark.memory

import java.lang.management.{ManagementFactory, MemoryMXBean}

import com.codahale.metrics.{Gauge, MetricRegistry}

import org.apache.spark.SparkConf
import org.apache.spark.metrics.source.Source

private[spark]
class MemoryManagerSource(val memoryManager: MemoryManager, conf: SparkConf)
  extends Source {

  override val metricRegistry = new MetricRegistry()

  override val sourceName = "memory_manager"

  val memoryFraction = conf.getDouble("spark.memory.fraction", 0.6)

  val runtimeMaxMemory = Runtime.getRuntime.maxMemory

  val usableMemory = runtimeMaxMemory - 300 * 1024 * 1024L

  metricRegistry.register(MetricRegistry.name("systemMemory"), new Gauge[Long] {
    override def getValue: Long = runtimeMaxMemory
  })

  metricRegistry.register(MetricRegistry.name("usableMemory"), new Gauge[Long] {
    override def getValue: Long = usableMemory
  })

  metricRegistry.register(MetricRegistry.name("maxHeapMemory"), new Gauge[Long] {
    override def getValue: Long = memoryManager.asInstanceOf[UnifiedMemoryManager].maxHeapMemory
  })

  metricRegistry.register(MetricRegistry.name("maxOffHeapMemory"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getMaxOffHeapMemory
  })

  metricRegistry.register(MetricRegistry.name("offHeapStorageRegionSize"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOffHeapStorageRegionSize
  })

  metricRegistry.register(MetricRegistry.name("onHeapStorageRegionSize"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOnHeapStorageRegionSize
  })

  metricRegistry.register(MetricRegistry.name("maxOnHeapStorageMemory"), new Gauge[Long] {
    override def getValue: Long = memoryManager.maxOnHeapStorageMemory
  })

  metricRegistry.register(MetricRegistry.name("onHeapStorageUsed"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOnHeapStorageUsed
  })

  metricRegistry.register(MetricRegistry.name("onHeapStorageFree"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOnHeapStorageFree
  })

  metricRegistry.register(MetricRegistry.name("onHeapExecutionUsed"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOnHeapExecutionUsed
  })

  metricRegistry.register(MetricRegistry.name("onHeapExecutionFree"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOnHeapExecutionFree
  })

  metricRegistry.register(MetricRegistry.name("offHeapStorageUsed"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOffHeapStorageUsed
  })

  metricRegistry.register(MetricRegistry.name("offHeapStorageFree"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOffHeapStorageFree
  })

  metricRegistry.register(MetricRegistry.name("offHeapExecutionUsed"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOffHeapExecutionUsed
  })

  metricRegistry.register(MetricRegistry.name("offHeapExecutionFree"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getOffHeapExecutionFree
  })

  metricRegistry.register(MetricRegistry.name("otherMemoryTotal"), new Gauge[Long] {
    override def getValue: Long =
      (usableMemory * (1.0 - memoryFraction)).toLong + 300 * 1024 * 1024L
  })

  metricRegistry.register(MetricRegistry.name("otherMemoryFree"), new Gauge[Long] {
    val memoryMXBean: MemoryMXBean = ManagementFactory.getMemoryMXBean()
    val heapUsed = memoryMXBean.getHeapMemoryUsage().getUsed()
    override def getValue: Long = (runtimeMaxMemory - heapUsed) -
      (memoryManager.getOnHeapStorageFree + memoryManager.getOnHeapExecutionFree)
  })

  metricRegistry.register(MetricRegistry.name("MXBeanHeapMemoryUsageUsed"), new Gauge[Long] {
    val memoryMXBean: MemoryMXBean = ManagementFactory.getMemoryMXBean()
    override def getValue: Long = memoryMXBean.getHeapMemoryUsage().getUsed()
  })

  metricRegistry.register(MetricRegistry.name("MXBeanHeapMemoryUsageMax"), new Gauge[Long] {
    val memoryMXBean: MemoryMXBean = ManagementFactory.getMemoryMXBean()
    override def getValue: Long = memoryMXBean.getHeapMemoryUsage().getMax()
  })

  // -- spill count metrics -----------------------------------------------------------------------
  metricRegistry.register(MetricRegistry.name("acquiredExecutionBytes"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getAcquiredBytes
  })

  metricRegistry.register(MetricRegistry.name("spilledTimes"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getSpilledTimes
  })

  metricRegistry.register(MetricRegistry.name("spilledBytes"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getSpilledBytes
  })

  // -- put and evict block count metrics ---------------------------------------------------------
  metricRegistry.register(MetricRegistry.name("putBlockNum"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getPutBlockNum
  })

  metricRegistry.register(MetricRegistry.name("putBlockBytes"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getPutBlockBytes
  })

  metricRegistry.register(MetricRegistry.name("evictBlockNum"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getEvictBlockNum
  })

  metricRegistry.register(MetricRegistry.name("evictBlockBytes"), new Gauge[Long] {
    override def getValue: Long = memoryManager.getEvictBlockBytes
  })

  // Get jvm mem:
  // Runtime.getRuntime.maxMemory
  // MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean()
  // memoryMXBean.getHeapMemoryUsage()
  // memoryMXBean.getNonHeapMemoryUsage()
  // memoryMXBean.getHeapMemoryUsage().getMax()
  // memoryMXBean.getHeapMemoryUsage().getUsed()
}
