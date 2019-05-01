/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.util;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.ParametersAreNonnullByDefault;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Utility class that allows messages to be queued and processed in the
 * background. Messages are accumulated in the queue, and processed periodically
 *
 * @param <T> The type of message
 * @author wwong
 */
public abstract class BatchScheduler<K, T>
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  // The queue of messages yet to be processed
  private final LoadingCache<K, BlockingQueue<T>> messageQueues;

  // The worker pool
  private final ExecutorService threadPool;

  /**
   * Constructor for the scheduler
   * @param threadPoolCount Number of worker threads to use
   */
  public BatchScheduler(int threadPoolCount)
  {
    Preconditions.checkArgument(threadPoolCount > 0);
    this.threadPool = Executors.newScheduledThreadPool(threadPoolCount);
    this.messageQueues = CacheBuilder.newBuilder()
        .removalListener(
            (RemovalListener<K, BlockingQueue<T>>)
                removal -> log.info(String.format("Removing queue %s from cache",
                                                  removal.getKey())))
        .build(
            new CacheLoader<K, BlockingQueue<T>>()
            {
              @ParametersAreNonnullByDefault
              public BlockingQueue<T> load(K key)
              {
                LinkedBlockingQueue<T> queue = new LinkedBlockingQueue<>();
                threadPool.submit(() -> doWork(key, queue));
                return queue;
              }
            });
  }

  /**
   * Enqueues a message to be collected and batched
   * @param message the message
   */
  public void enqueueMessage(T message)
  {
    Preconditions.checkNotNull(message);
    Queue<T> messageQueue;
    try
    {
      messageQueue = messageQueues.get(getKeyFromMessage(message));
    }
    catch (ExecutionException e)
    {
      log.error("Could not initialize queue " + e);
      return;
    }
    Preconditions.checkNotNull(messageQueue);
    log.info("Enqueueing message. Current count: " + messageQueue.size());
    messageQueue.add(message);
  }

  private void doWork(K key, LinkedBlockingQueue<T> queue)
  {
    try
    {
      if (processMessages(queue))
      {
        threadPool.submit(() -> doWork(key, queue));
      }
      else
      {
        messageQueues.invalidate(key);
      }
    }
    catch (Throwable t)
    {
      // Skip the queue message and continue
      threadPool.submit(() -> doWork(key, queue));
    }
  }

  // Use a queue, since iterators have no guaranteed behavior when elements
  // are added after the iterator is created
  public abstract boolean processMessages(BlockingQueue<T> messages);

  public abstract K getKeyFromMessage(T message);
}
