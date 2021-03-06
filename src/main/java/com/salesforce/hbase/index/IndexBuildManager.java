/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.MiniBatchOperationInProgress;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Threads;

import com.salesforce.hbase.index.builder.IndexBuilder;
import com.salesforce.hbase.index.builder.IndexBuildingFailureException;
import com.salesforce.hbase.index.parallel.EarlyExitFailure;
import com.salesforce.hbase.index.parallel.QuickFailingTaskRunner;
import com.salesforce.hbase.index.parallel.Task;
import com.salesforce.hbase.index.parallel.TaskBatch;

/**
 * Manage the building of index updates from primary table updates.
 * <p>
 * Internally, parallelizes updates through a thread-pool to a delegate index builder. Underlying
 * {@link IndexBuilder} <b>must be thread safe</b> for each index update.
 */
public class IndexBuildManager implements Stoppable {

  private static final Log LOG = LogFactory.getLog(IndexBuildManager.class);
  private final IndexBuilder delegate;
  private QuickFailingTaskRunner pool;
  private boolean stopped;

  /**
   * Set the number of threads with which we can concurrently build index updates. Unused threads
   * will be released, but setting the number of threads too high could cause frequent swapping and
   * resource contention on the server - <i>tune with care</i>. However, if you are spending a lot
   * of time building index updates, it could be worthwhile to spend the time to tune this parameter
   * as it could lead to dramatic increases in speed.
   */
  public static String NUM_CONCURRENT_INDEX_BUILDER_THREADS_CONF_KEY = "index.builder.threads.max";
  /** Default to a single thread. This is the safest course of action, but the slowest as well */
  private static final int DEFAULT_CONCURRENT_INDEX_BUILDER_THREADS = 10;
  /**
   * Amount of time to keep idle threads in the pool. After this time (seconds) we expire the
   * threads and will re-create them as needed, up to the configured max
   */
  private static final String INDEX_BUILDER_KEEP_ALIVE_TIME_CONF_KEY =
      "index.builder.threads.keepalivetime";

  /**
   * @param env environment in which <tt>this</tt> is running. Used to setup the
   *          {@link IndexBuilder} and executor
   * @throws IOException if an {@link IndexBuilder} cannot be correctly steup
   */
  public IndexBuildManager(RegionCoprocessorEnvironment env) throws IOException {
    this(getIndexBuilder(env), new QuickFailingTaskRunner(
        getDefaultExecutor(env.getConfiguration())));
  }

  private static IndexBuilder getIndexBuilder(RegionCoprocessorEnvironment e) throws IOException {
    Configuration conf = e.getConfiguration();
    Class<? extends IndexBuilder> builderClass =
        conf.getClass(Indexer.INDEX_BUILDER_CONF_KEY, null, IndexBuilder.class);
    try {
      IndexBuilder builder = builderClass.newInstance();
      builder.setup(e);
      return builder;
    } catch (InstantiationException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + e.getRegion().getTableDesc().getNameAsString());
    } catch (IllegalAccessException e1) {
      throw new IOException("Couldn't instantiate index builder:" + builderClass
          + ", disabling indexing on table " + e.getRegion().getTableDesc().getNameAsString());
    }
  }

  public IndexBuildManager(IndexBuilder builder, QuickFailingTaskRunner pool) {
    this.delegate = builder;
    this.pool = pool;
  }

  /**
   * @param conf to read
   * @return a thread pool based on the passed configuration whose threads are all daemon threads.
   */
  private static ThreadPoolExecutor getDefaultExecutor(Configuration conf) {
    int maxThreads =
        conf.getInt(NUM_CONCURRENT_INDEX_BUILDER_THREADS_CONF_KEY,
          DEFAULT_CONCURRENT_INDEX_BUILDER_THREADS);
    if (maxThreads == 0) {
      maxThreads = 1; // is there a better default?
    }
    LOG.info("Starting builder with " + maxThreads + " threads for all tables");
    long keepAliveTime = conf.getLong(INDEX_BUILDER_KEEP_ALIVE_TIME_CONF_KEY, 60);

    // we prefer starting a new thread to queuing (the opposite of the usual ThreadPoolExecutor)
    // since we are probably writing to a bunch of index tables in this case. Any pending requests
    // are then queued up in an infinite (Integer.MAX_VALUE) queue. However, we allow core threads
    // to timeout, to we tune up/down for bursty situations. We could be a bit smarter and more
    // closely manage the core-thread pool size to handle the bursty traffic (so we can always keep
    // some core threads on hand, rather than starting from scratch each time), but that would take
    // even more time. If we shutdown the pool, but are still putting new tasks, we can just do the
    // usual policy and throw a RejectedExecutionException because we are shutting down anyways and
    // the worst thing is that this coprocessor unloaded.
    ThreadPoolExecutor pool =
        new ThreadPoolExecutor(maxThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(), Threads.newDaemonThreadFactory("index-builder-"));
    pool.allowCoreThreadTimeOut(true);
    return pool;
  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(
      MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp,
      Collection<? extends Mutation> mutations)
      throws IOException {
    // notify the delegate that we have started processing a batch
    this.delegate.batchStarted(miniBatchOp);

    // parallelize each mutation into its own task
    // each task is cancelable via two mechanisms: (1) underlying HRegion is closing (which would
    // fail lookups/scanning) and (2) by stopping this via the #stop method. Interrupts will only be
    // acknowledged on each thread before doing the actual lookup, but after that depends on the
    // underlying builder to look for the closed flag.
    TaskBatch<Collection<Pair<Mutation, byte[]>>> tasks =
        new TaskBatch<Collection<Pair<Mutation, byte[]>>>(mutations.size());
    for (final Mutation m : mutations) {
      tasks.add(new Task<Collection<Pair<Mutation, byte[]>>>() {

        @Override
        public Collection<Pair<Mutation, byte[]>> call() throws Exception {
          return delegate.getIndexUpdate(m);
        }

      });
    }
    List<Collection<Pair<Mutation, byte[]>>> allResults = null;
    try {
      allResults = pool.submitUninterruptible(tasks);
    } catch (EarlyExitFailure e) {
      propagateFailure(e);
    } catch (ExecutionException e) {
      LOG.error("Found a failed index update!");
      propagateFailure(e.getCause());
    }

    // we can only get here if we get successes from each of the tasks, so each of these must have a
    // correct result
    Collection<Pair<Mutation, byte[]>> results = new ArrayList<Pair<Mutation, byte[]>>();
    for (Collection<Pair<Mutation, byte[]>> result : allResults) {
      assert result != null : "Found an unsuccessful result, but didn't propagate a failure earlier";
      results.addAll(result);
    }

    return results;
  }

  /**
   * Propagate the given failure as a generic {@link IOException}, if it isn't already
   * @param e failure
   */
  private void propagateFailure(Throwable e) throws IOException {
    try {
      throw e;
    } catch (IOException e1) {
      throw e1;
    } catch (Throwable e1) {
      throw new IndexBuildingFailureException("Failed to build index for unexpected reason!", e1);
    }
  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdate(Delete delete) throws IOException {
    // all we get is a single update, so it would probably just go slower if we needed to queue it
    // up. It will increase underlying resource contention a little bit, but the mutation case is
    // far more common, so let's not worry about it for now.
    // short circuit so we don't waste time.
    if (!this.delegate.isEnabled(delete)) {
      return null;
    }

    return delegate.getIndexUpdate(delete);
  }

  public Collection<Pair<Mutation, byte[]>> getIndexUpdateForFilteredRows(
      Collection<KeyValue> filtered) throws IOException {
    // this is run async, so we can take our time here
    return delegate.getIndexUpdateForFilteredRows(filtered);
  }

  public void batchCompleted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp) {
    delegate.batchCompleted(miniBatchOp);
  }

  public void batchStarted(MiniBatchOperationInProgress<Pair<Mutation, Integer>> miniBatchOp)
      throws IOException {
    delegate.batchStarted(miniBatchOp);
  }

  public boolean isEnabled(Mutation m) throws IOException {
    return delegate.isEnabled(m);
  }

  public byte[] getBatchId(Mutation m) {
    return delegate.getBatchId(m);
  }

  @Override
  public void stop(String why) {
    if (stopped) {
      return;
    }
    this.stopped = true;
    this.delegate.stop(why);
    this.pool.stop(why);
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }

  public IndexBuilder getBuilderForTesting() {
    return this.delegate;
  }
}