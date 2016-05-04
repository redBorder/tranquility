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

package com.metamx.tranquility.server.http

import com.fasterxml.jackson.dataformat.smile.SmileFactory
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes
import com.metamx.common.scala.Abort
import com.metamx.common.scala.Jackson
import com.metamx.common.scala.Logging
import com.metamx.common.scala.Walker
import com.metamx.common.scala.untyped.Dict
import com.metamx.tranquility.server.http.TranquilityServlet._
import com.metamx.tranquility.tranquilizer.BufferFullException
import com.metamx.tranquility.tranquilizer.MessageDroppedException
import com.metamx.tranquility.tranquilizer.Tranquilizer
import com.twitter.util.Return
import com.twitter.util.Throw
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.ws.rs.core.MediaType
import org.jboss.netty.handler.codec.http.HttpResponseStatus
import org.scalatra.ScalatraServlet
import scala.collection.JavaConverters._
import scala.collection.mutable

class TranquilityServlet(
  tranquilizers: Map[String, Tranquilizer[java.util.Map[String, AnyRef]]]
) extends ScalatraServlet with Logging
{
  get("/") {
    contentType = "text/plain"
    """
      |
      |
      |
      |                            ╒▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▄▄▄▄
      |                             ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█▓▓▓▄
      |                                                          ▀█▓▓╕
      |                                                             █▓▓
      |              ▄▓▓▓▓▓▄    ▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄▄          ▀▓▓
      |              ▀▀▀▀▀▀▀    ▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀▀█▓▓▄        ▀▓▓
      |                                                     ▀▓▓µ       ▓▓µ
      |                                                      ▐▓▓       ▓▓▄
      |                                                       ▓▓       ▓▓▄
      |                                                      ╒▓▓       ▓▓
      |                                                     ,▓▓Γ      ▄▓█
      |                                                    ▄▓▓Γ      ╓▓▓
      |                                               ,▄▄▓▓█▀       y▓▓
      |                     ▀▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓██▀▀         ▓▓█
      |                                                         ,▓▓▓▀
      |                                                      ,▄▓▓█▀
      |                                                ,▄▄▓▓▓▓█▀
      |                             └▓▓▓▓▓▓▓   ▀▓▓▓▓▓▓▓███▀▀
      |
      |
      | """.stripMargin
  }

  post("/v1/post") {
    val async = yesNo("async", false)
    doV1Post(None, async)
  }

  post("/v1/post/:dataSource") {
    val async = yesNo("async", false)
    doV1Post(Some(params("dataSource")), async)
  }

  notFound {
    status = 404
    contentType = "text/plain"
    "Not found\n"
  }

  error {
    case e: HttpException =>
      log.debug(e, s"User error serving request to ${request.uri}")
      status = e.status.getCode
      contentType = "text/plain"
      e.message + "\n"

    case e: Exception =>
      log.warn(e, s"Server error serving request to ${request.uri}")
      status = 500
      contentType = "text/plain"
      "Server error\n"

    case e: Throwable =>
      Abort(e)
  }

  private def getObjectMapper() = request.contentType match {
    case Some(JsonContentType) => JsonObjectMapper
    case Some(SmileContentType) => SmileObjectMapper
    case _ =>
      throw new HttpException(
        HttpResponseStatus.BAD_REQUEST,
        s"Expected contentType $JsonContentType or $SmileContentType"
      )
  }

  private def doV1Post(dataSource: Option[String], async: Boolean): Array[Byte] = {
    val objectMapper = getObjectMapper()
    val messages = Messages.fromInputStreamV1(objectMapper, request.inputStream, dataSource)
    val (received, sent) = doSend(messages, async)
    val result = Dict("result" -> Dict("received" -> received, "sent" -> sent))
    contentType = request.contentType.get
    objectMapper.writeValueAsBytes(result)
  }

  private def doSend(messages: Walker[(String, Dict)], async: Boolean): (Long, Long) = {
    val senders = mutable.HashMap[String, Tranquilizer[java.util.Map[String, AnyRef]]]()
    val received = new AtomicLong
    val sent = new AtomicLong
    val exception = new AtomicReference[Throwable]
    for ((dataSource, message) <- messages) {
      val sender = senders.getOrElseUpdate(
        dataSource, {
          tranquilizers.getOrElse(
            dataSource, {
              throw new HttpException(HttpResponseStatus.BAD_REQUEST, s"No beam defined for dataSource '$dataSource'")
            }
          )
        }
      )

      received.incrementAndGet()

      val future = try {
        sender.send(message.asJava.asInstanceOf[java.util.Map[String, AnyRef]])
      }
      catch {
        case e: BufferFullException =>
          throw new HttpException(HttpResponseStatus.SERVICE_UNAVAILABLE, s"Buffer full for dataSource '$dataSource'")
      }

      future respond {
        case Return(_) => sent.incrementAndGet()
        case Throw(e: MessageDroppedException) => // Suppress
        case Throw(e) => exception.compareAndSet(null, e)
      }

      // async => ignore sent, exception; just receive things.
      if (!async && exception.get() != null) {
        throw exception.get()
      }
    }

    // async => ignore sent, exception; just receive things.
    if (!async) {
      senders.values.foreach(_.flush())

      if (exception.get() != null) {
        throw exception.get()
      }
    }

    (received.get(), if (async) 0L else sent.get())
  }

  private def yesNo(k: String, defaultValue: Boolean): Boolean = {
    request.parameters.get(k) map { s =>
      s.toLowerCase() match {
        case "true" | "1" | "yes" => true
        case "false" | "0" | "no" | "" => false
        case _ =>
          throw new HttpException(HttpResponseStatus.BAD_REQUEST, "Expected true or false")
      }
    } getOrElse defaultValue
  }

}

object TranquilityServlet
{
  val JsonObjectMapper  = Jackson.newObjectMapper()
  val SmileObjectMapper = Jackson.newObjectMapper(new SmileFactory)

  val JsonContentType  = MediaType.APPLICATION_JSON
  val SmileContentType = SmileMediaTypes.APPLICATION_JACKSON_SMILE
}
