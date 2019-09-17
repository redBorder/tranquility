/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.metamx.tranquility.samza

import com.metamx.common.scala.Logging
import com.metamx.common.scala.Predef.EffectOps
import com.metamx.common.scala.collection.mutable.ConcurrentMap
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import java.util.concurrent.atomic.AtomicReference
import org.apache.samza.config.Config
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemProducer
import org.apache.samza.system.SystemStream
import scala.collection.mutable

class BeamProducer(
  beamFactory: BeamFactory,
  systemName: String,
  config: Config,
  batchSize: Int,
  maxPendingBatches: Int,
  lingerMillis: Long,
  throwOnError: Boolean
) extends SystemProducer with Logging
{
  def this(
    beamFactory: BeamFactory,
    systemName: String,
    config: Config,
    batchSize: Int,
    maxPendingBatches: Int,
    throwOnError: Boolean
  ) = this(
    beamFactory,
    systemName,
    config,
    batchSize,
    maxPendingBatches,
    Tranquilizer.DefaultLingerMillis,
    throwOnError
  )

  // stream => sender
  private val senders = ConcurrentMap[String, Tranquilizer[Any]]()

  private val exception = new AtomicReference[Throwable]()

   // stream => current stream partitions
  private val partitionsRef = mutable.Map[String, Array[Int]]()

  override def start() {}

  override def stop() {
    senders.values.foreach(_.stop())
  }

  override def register(source: String) {}

  override def send(source: String, envelope: OutgoingMessageEnvelope) {
    val streamName = envelope.getSystemStream.getStream
    val message = envelope.getMessage
    val partitionsReplicas = envelope.getKey.asInstanceOf[Array[Int]]

    // Get the previous partitions from stream or create it
    val partitionRef = partitionsRef.getOrElseUpdate(streamName, {
      log.info("Creating new partitionsRef[%s] to datasource[%s]", "%s_%s".format(partitionsReplicas(0), partitionsReplicas(1)), streamName)
      partitionsReplicas
    })

    // Check if the previous state partitions is the same that current desire partitions
    //  * If it is different clean and stop the sender, and update partitionsRef.
    if (!(partitionRef sameElements partitionsReplicas)) {
      senders.remove(streamName) match {
        case Some(t) =>
          log.info("Stopping sender[%s] with partitionRef[%s]", streamName, "%s_%s".format(partitionRef(0), partitionRef(1)))
          t.stop()
        case None =>
          log.info("Stream[%s] doesn't exists!", streamName)
      }
      partitionsRef.update(streamName, partitionsReplicas)
    }

    // Get sender from stream or create it with desire partitions.
    val sender = senders.get(streamName) match {
      case Some(x) => x
      case None =>
        senders.synchronized {
          senders.getOrElseUpdate(
            streamName, {
               log.info("Creating beam for stream[%s.%s] with partitions[%s] and replicas[%s].",
          systemName, streamName, partitionsReplicas(0), partitionsReplicas(1))
              Tranquilizer.create(
                beamFactory.makeBeam(new SystemStream(systemName, streamName), partitionsReplicas(0), partitionsReplicas(1), config),
                batchSize,
                maxPendingBatches,
                lingerMillis
              ) withEffect(_.start())
            }
          )
        }
    }

    sender.send(message) handle {
      case _: MessageDroppedException => // Suppress
      case e => exception.compareAndSet(null, e)
    }

    maybeThrow()
  }

  override def flush(source: String) {
    // So flippin' lazy. Flush ALL the data!
    senders.values.foreach(_.flush())
    maybeThrow()
  }

  private def maybeThrow() {
    if (exception.get() != null) {
      throw exception.get()
    }
  }
}
