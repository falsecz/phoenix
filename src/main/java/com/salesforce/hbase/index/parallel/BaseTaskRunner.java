/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc. All rights reserved. Redistribution and use in source
 * and binary forms, with or without modification, are permitted provided that the following
 * conditions are met: Redistributions of source code must retain the above copyright notice, this
 * list of conditions and the following disclaimer. Redistributions in binary form must reproduce
 * the above copyright notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution. Neither the name of
 * Salesforce.com nor the names of its contributors may be used to endorse or promote products
 * derived from this software without specific prior written permission. THIS SOFTWARE IS PROVIDED
 * BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
 * INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF
 * THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.hbase.index.parallel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * {@link TaskRunner} that just manages the underlying thread pool. On called to
 * {@link #stop(String)}, the thread pool is shutdown immediately - all pending tasks are cancelled
 * and running tasks receive and interrupt.
 * <p>
 * If we find a failure the failure is propagated to the {@link TaskBatch} so any {@link Task} that
 * is interested can kill itself as well.
 */
public abstract class BaseTaskRunner implements TaskRunner {

  private static final Log LOG = LogFactory.getLog(BaseTaskRunner.class);
  protected ListeningExecutorService writerPool;
  private boolean stopped;

  public BaseTaskRunner(ExecutorService service) {
    this.writerPool = MoreExecutors.listeningDecorator(service);
  }

  @Override
  public <R> List<R> submit(TaskBatch<R> tasks) throws CancellationException, ExecutionException,
      InterruptedException {
    // submit each task to the pool and queue it up to be watched
    List<ListenableFuture<R>> futures = new ArrayList<ListenableFuture<R>>(tasks.size());
    for (Task<R> task : tasks.getTasks()) {
      futures.add(this.writerPool.submit(task));
    }
    try {
      // This logic is actually much more synchronized than the previous logic. Now we rely on a
      // synchronization around the status to tell us when we are done. While this does have the
      // advantage of being (1) less code, and (2) supported as part of a library, it is just that
      // little bit slower. If push comes to shove, we can revert back to the previous
      // implementation, but for right now, this works just fine.
      return submitTasks(futures).get();
    } catch (CancellationException e) {
      // propagate the failure back out
      String msg = "Found a failed task!";
      LOG.error(msg, e);
      tasks.abort(msg, e.getCause());
      throw e;
    } catch (ExecutionException e) {
      // propagate the failure back out
      String msg = "Found a failed task!";
      LOG.error(msg, e);
      tasks.abort(msg, e.getCause());
      throw e;
    }
  }

  /**
   * Build a ListenableFuture for the tasks. Implementing classes can determine return behaviors on
   * the given tasks
   * @param futures to wait on
   * @return a single {@link ListenableFuture} that completes based on the passes tasks.
   */
  protected abstract <R> ListenableFuture<List<R>> submitTasks(List<ListenableFuture<R>> futures);

  @Override
  public <R> List<R> submitUninterruptible(TaskBatch<R> tasks) throws EarlyExitFailure,
      ExecutionException {
    boolean interrupted = false;
    try {
      while (!this.isStopped()) {
        try {
          return this.submit(tasks);
        } catch (InterruptedException e) {
          interrupted = true;
        }
      }
    } finally {
      // restore the interrupted status
      if (interrupted) {
        Thread.currentThread().interrupt();
      }
    }

    // should only get here if we are interrupted while waiting for a result and have been told to
    // shutdown by an external source
    throw new EarlyExitFailure("Interrupted and stopped before computation was complete!");
  }

  @Override
  public void stop(String why) {
    if (this.stopped) {
      return;
    }
    LOG.info("Shutting down task runner because " + why);
    this.writerPool.shutdownNow();
  }

  @Override
  public boolean isStopped() {
    return this.stopped;
  }
}