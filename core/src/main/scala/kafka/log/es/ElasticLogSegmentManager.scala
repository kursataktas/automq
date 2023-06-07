/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.log.es

import com.automq.elasticstream.client.api
import kafka.log.es.ElasticLog.info

import java.util
import java.util.stream.Collectors

class ElasticLogSegmentManager(val metaStream: api.Stream, val streamManager: ElasticLogStreamManager, val logIdent: String) {
  val segments = new java.util.concurrent.ConcurrentHashMap[Long, ElasticLogSegment]()
  val segmentEventListener = new EventListener()

  def put(baseOffset: Long, segment: ElasticLogSegment): Unit = {
    segments.put(baseOffset, segment)
  }

  def remove(baseOffset: Long): ElasticLogSegment = {
    segments.remove(baseOffset)
  }

  def persistLogMeta(): Unit = {
    val meta = logMeta()
    val kv = MetaKeyValue.of(ElasticLog.LOG_META_KEY, ElasticLogMeta.encode(meta))
    metaStream.append(RawPayloadRecordBatch.of(MetaKeyValue.encode(kv))).get()
    info(s"${logIdent}save log meta $meta")

  }

  def logSegmentEventListener(): ElasticLogSegmentEventListener = {
    segmentEventListener
  }

  def logMeta(): ElasticLogMeta = {
    val elasticLogMeta = new ElasticLogMeta()
    val streamMap = new util.HashMap[String, java.lang.Long]()
    streamManager.streams.entrySet().forEach(entry => {
      streamMap.put(entry.getKey, entry.getValue.streamId())
    })
    elasticLogMeta.setStreamMap(streamMap)
    val segmentList: util.List[ElasticStreamSegmentMeta] = segments.values.stream().map(segment => segment.meta).collect(Collectors.toList())
    elasticLogMeta.setSegmentMetas(segmentList)
    elasticLogMeta
  }

  class EventListener extends ElasticLogSegmentEventListener {
    override def onEvent(segmentBaseOffset: Long, event: ElasticLogSegmentEvent): Unit = {
      event match {
        case ElasticLogSegmentEvent.SEGMENT_DELETE =>
          val deleted = remove(segmentBaseOffset) != null
          if (deleted) {
            persistLogMeta()
          }
        case ElasticLogSegmentEvent.SEGMENT_UPDATE =>
          persistLogMeta()
        case _ =>
          throw new IllegalStateException(s"Unsupported event $event")
      }
    }
  }

}