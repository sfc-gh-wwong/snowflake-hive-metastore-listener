/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.google.common.base.Preconditions;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Utility class that allows messages to be queued and processed in the
 * background. Messages are accumulated in the queue, and processed periodically
 *
 * @param <T> The type of message
 * @author wwong
 */
public class BatchScheduler<T>
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  // The queue of messages yet to be processed
  private final BlockingQueue<T> messageQueue;

  // The worker pool
  private final ExecutorService threadPool;

  /**
   * Constructor for the scheduler
   * @param threadPoolCount Number of worker threads to use
   * @param batchingPeriodMs Duration of time between processing the batches
   * @param processMessages Method that processes the messages, if given the
   *                        message queue
   */
  public BatchScheduler(int threadPoolCount,
                        int batchingPeriodMs,
                        BiConsumer<Queue<T>, BatchScheduler<T>> processMessages)
  {
    Preconditions.checkArgument(threadPoolCount > 0);
    Preconditions.checkArgument(batchingPeriodMs > 0);
    Preconditions.checkNotNull(processMessages);
    this.messageQueue = new LinkedBlockingQueue<>();

    // Additional initialization
    threadPool = Executors.newFixedThreadPool(threadPoolCount);

    log.info(String.format("Starting schedule with a batching period of %s ms",
                           batchingPeriodMs));
    ScheduledExecutorService scheduledExecutor =
        Executors.newSingleThreadScheduledExecutor();
    // Notes:
    //  - Executes a recurring action in a timely fashion (no drift)
    //  - If an action takes longer than the period, all future actions are late
    //  - If uncaught, an exception would stop future actions
    scheduledExecutor.scheduleAtFixedRate(() ->
    {
      try
      {
        processMessages.accept(messageQueue, this);
      }
      catch (Throwable t)
      {
        log.warn("Hit exception running scheduled action: " + t);
      }
    }, 0, batchingPeriodMs, TimeUnit.MILLISECONDS);
    log.info("Started scheduler");
  }

  /**
   * Enqueues a message to be collected and batched
   * @param message the message
   */
  public void enqueueMessage(T message)
  {
    Preconditions.checkNotNull(message);
    log.info("Enqueueing message. Current count: " + messageQueue.size());
    messageQueue.add(message);
  }

  /**
   * Submits a task to a thread pool
   * @param task the task to be executed
   */
  public void submitTask(Runnable task)
  {
    Preconditions.checkNotNull(task);
    log.info("Task received");
    threadPool.submit(task);
  }
}
