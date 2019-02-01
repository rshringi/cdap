/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.pipeline.queue;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.pipeline.LogPipelineConfig;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * The {@link TimeEventQueue} processor to enqueue the log events to {@link TimeEventQueue}, and process them.
 * @param <Offset> type of the offset
 */
public class TimeEventQueueProcessor<Offset extends Comparable<Offset>> {
  private static final Logger LOG = LoggerFactory.getLogger(TimeEventQueueProcessor.class);
  private static final double MIN_FREE_FACTOR = 0.5d;
  private final String name;
  private final TimeEventQueue<ILoggingEvent, Offset> eventQueue;
  private final LogProcessorPipelineContext context;
  private final MetricsContext metricsContext;
  private final LogPipelineConfig config;

  /**
   * Time event queue processor.
   */
  public TimeEventQueueProcessor(String name, LogProcessorPipelineContext context, LogPipelineConfig config,
                                 Iterable<Integer> partitions) {
    this.name = name;
    this.eventQueue = new TimeEventQueue<>(partitions);
    this.context = context;
    this.metricsContext = context;
    this.config = config;
  }

  /**
   * Processes events provided by event iterator for a given partition.
   *
   * @param partition log event partition
   * @param eventIterator log events iterator
   * @param eventConverter event converter to convert log event to {@code ProcessorEvent}
   * @param <T> type of the event provided by the iterator
   *
   * @return processed event metadata
   */
  public <T> ProcessedEventMetadata<Offset> process(int partition, Iterator<T> eventIterator,
                                                    Function<T, ProcessorEvent<Offset>> eventConverter) {
    while (eventIterator.hasNext()) {
      // queue is full can not enqueue more events
      if (getMaxRetainSize() != Long.MAX_VALUE) {
        break;
      }

      // if the queue is not full, enqueue the log event
      T event = eventIterator.next();
      ProcessorEvent<Offset> processorEvent = eventConverter.apply(event);
      eventQueue.add(processorEvent.getEvent(), processorEvent.getEvent().getTimeStamp(), processorEvent.getEventSize(),
                     partition, processorEvent.getOffset());
    }

    // if event queue is full or all the events have been added to the queue, append all the enqueued events to log
    // appenders.
    return append();
  }

  private ProcessedEventMetadata<Offset> append() {
    long minEventTime = System.currentTimeMillis() - config.getEventDelayMillis();
    long maxRetainSize = getMaxRetainSize();

    int eventsAppended = 0;
    long minDelay = Long.MAX_VALUE;
    long maxDelay = -1;
    Map<Integer, Checkpoint<Offset>> metadata = new HashMap<>();

    TimeEventQueue.EventIterator<ILoggingEvent, Offset> iterator = eventQueue.iterator();
    while (iterator.hasNext()) {
      ILoggingEvent event = iterator.next();

      // If not forced to reduce the event queue size and the current event timestamp is still within the
      // buffering time, no need to iterate anymore
      if (eventQueue.getEventSize() <= maxRetainSize && event.getTimeStamp() >= minEventTime) {
        break;
      }

      // update delay
      long delay = System.currentTimeMillis() - event.getTimeStamp();
      minDelay = delay < minDelay ? delay : minDelay;
      maxDelay = delay > maxDelay ? delay : maxDelay;

      try {
        // Otherwise, append the event
        ch.qos.logback.classic.Logger effectiveLogger = context.getEffectiveLogger(event.getLoggerName());
        if (event.getLevel().isGreaterOrEqual(effectiveLogger.getEffectiveLevel())) {
          effectiveLogger.callAppenders(event);
        }
      } catch (Exception e) {
        LOG.warn("Failed to append log event in pipeline {}. Will be retried.", name, e);
        break;
      }

      metadata.put(iterator.getPartition(), new Checkpoint<>(eventQueue.getSmallestOffset(iterator.getPartition()),
                                                             event.getTimeStamp()));
      iterator.remove();
      eventsAppended++;
    }

    // Always try to call flush, even there was no event written. This is needed so that appender get called
    // periodically even there is no new events being appended to perform housekeeping work.
    // Failure to flush is ok and it will be retried by the wrapped appender
    try {
      context.flush();
    } catch (IOException e) {
      LOG.warn("Failed to flush in pipeline {}. Will be retried.", name, e);
    }
    metricsContext.gauge("event.queue.size.bytes", eventQueue.getEventSize());

    // If no event was appended and the buffer is not full, so just return with 0 events appended.
    if (eventsAppended == 0) {
      // nothing has been appended so there is no metadata associated with those events. So return null.
      return new ProcessedEventMetadata<>(0, null);
    }

    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MIN_DELAY, minDelay);
    metricsContext.gauge(Constants.Metrics.Name.Log.PROCESS_MAX_DELAY, maxDelay);
    metricsContext.increment(Constants.Metrics.Name.Log.PROCESS_MESSAGES_COUNT, eventsAppended);

    return new ProcessedEventMetadata<>(eventsAppended, metadata);
  }

  /**
   * Calculates max retain size for event queue.
   */
  private long getMaxRetainSize() {
    long maxRetainSize = eventQueue.getEventSize() >= config.getMaxBufferSize() ?
      (long) (config.getMaxBufferSize() * MIN_FREE_FACTOR) : Long.MAX_VALUE;

    if (maxRetainSize != Long.MAX_VALUE) {
      LOG.info("Maximum queue size {} reached for pipeline {}.", config.getMaxBufferSize(), name);
    }
    return maxRetainSize;
  }

  /**
   * Returns if the queue is empty for a given partition.
   */
  public boolean isQueueEmpty(int partition) {
    return eventQueue.isEmpty(partition);
  }
}
