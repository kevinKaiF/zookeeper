/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Flushable;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 将请求log到磁盘，高效的批量io处理。当请求同步到磁盘才会传递给下个processor
 *
 * This RequestProcessor logs requests to disk. It batches the requests to do
 * the io efficiently. The request is not passed to the next RequestProcessor
 * until its log has been synced to disk.
 *
 * SyncRequestProcessor is used in 3 different cases
 * 1. Leader - Sync request to disk and forward it to AckRequestProcessor which
 *             send ack back to itself.
 * 2. Follower - Sync request to disk and forward request to
 *             SendAckRequestProcessor which send the packets to leader.
 *             SendAckRequestProcessor is flushable which allow us to force
 *             push packets to leader.
 * 3. Observer - Sync committed request to disk (received as INFORM packet).
 *             It never send ack back to the leader, so the nextProcessor will
 *             be null. This change the semantic of txnlog on the observer
 *             since it only contains committed txns.
 */

/**
 * 处理来自follower,observer，或者leader本身的request记录到日志
 */
public class SyncRequestProcessor extends ZooKeeperCriticalThread implements
        RequestProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(SyncRequestProcessor.class);
    private final ZooKeeperServer zks;
    private final LinkedBlockingQueue<Request> queuedRequests =
        new LinkedBlockingQueue<Request>();
    private final RequestProcessor nextProcessor;

    private Thread snapInProcess = null;
    volatile private boolean running;

    /**
     * Transactions that have been written and are waiting to be flushed to
     * disk. Basically this is the list of SyncItems whose callbacks will be
     * invoked after flush returns successfully.
     */
    // 批量处理
    private final LinkedList<Request> toFlush = new LinkedList<Request>();
    private final Random r = new Random(System.nanoTime());
    /**
     * The number of log entries to log before starting a snapshot
     */
    // 内存中日志的批量数目
    private static int snapCount = ZooKeeperServer.getSnapCount();

    private final Request requestOfDeath = Request.requestOfDeath;

    public SyncRequestProcessor(ZooKeeperServer zks,
            RequestProcessor nextProcessor) {
        super("SyncThread:" + zks.getServerId(), zks
                .getZooKeeperServerListener());
        this.zks = zks;
        this.nextProcessor = nextProcessor;
        running = true;
    }

    /**
     * used by tests to check for changing
     * snapcounts
     * @param count
     */
    public static void setSnapCount(int count) {
        snapCount = count;
    }

    /**
     * used by tests to get the snapcount
     * @return the snapcount
     */
    public static int getSnapCount() {
        return snapCount;
    }

    @Override
    public void run() {
        try {
            int logCount = 0;

            // we do this in an attempt to ensure that not all of the servers
            // in the ensemble take a snapshot at the same time
            // 只刷出一部分日志
            int randRoll = r.nextInt(snapCount/2);
            while (true) {
                Request si = null;
                if (toFlush.isEmpty()) {
                    // 如果队列是空的，take进行阻塞，否则直接poll
                    si = queuedRequests.take();
                } else {
                    si = queuedRequests.poll();
                    // 请求队列为空，则刷出到下个requestProcessor
                    if (si == null) {
                        flush(toFlush);
                        continue;
                    }
                }
                // 请求中止
                if (si == requestOfDeath) {
                    break;
                }
                if (si != null) {
                    // track the number of records written to the log
                    // 将Request的TxnHeader和Record序列化到FileTxnSnapLog目录
                    // 如果没有TxnHeader，则不记录到日志

                    // 追加log日志记录
                    // log记录了所有请求的日志数据，而snapshot只记录了当前zkDb的内存数据
                    // 如果lastProcessedZxid有变更，snapshot会生成新的快照日志文件
                    // 写入快照文件的文件头，写入当前的session数据，写入当前的dataTree数据

                    // 这些日志都是写入请求，所以zxid会递增，roll之后会生成新的日志文件
                    if (zks.getZKDatabase().append(si)) {
                        logCount++;
                        // 如果大半数目，则刷出日志
                        if (logCount > (snapCount / 2 + randRoll)) {
                            randRoll = r.nextInt(snapCount/2);
                            // roll the log
                            // 刷出日志
                            zks.getZKDatabase().rollLog();
                            // take a snapshot
                            if (snapInProcess != null && snapInProcess.isAlive()) {
                                LOG.warn("Too busy to snap, skipping");
                            } else {
                                snapInProcess = new ZooKeeperThread("Snapshot Thread") {
                                        public void run() {
                                            try {
                                                // 打印快照，持久化日志到FileTxnSnapLog
                                                zks.takeSnapshot();
                                            } catch(Exception e) {
                                                LOG.warn("Unexpected exception", e);
                                            }
                                        }
                                    };
                                snapInProcess.start();
                            }
                            // 清空统计
                            logCount = 0;
                        }
                    } else if (toFlush.isEmpty()) {
                        // optimization for read heavy workloads
                        // iff this is a read, and there are no pending
                        // flushes (writes), then just pass this to the next
                        // processor
                        /**
                         * {@link org.apache.zookeeper.server.quorum.AckRequestProcessor}
                         */
                        if (nextProcessor != null) {
                            nextProcessor.processRequest(si);
                            if (nextProcessor instanceof Flushable) {
                                ((Flushable)nextProcessor).flush();
                            }
                        }
                        continue;
                    }

                    // 刷新日志数据
                    toFlush.add(si);
                    if (toFlush.size() > 1000) {
                        flush(toFlush);
                    }
                }
            }
        } catch (Throwable t) {
            handleException(this.getName(), t);
        } finally{
            running = false;
        }
        LOG.info("SyncRequestProcessor exited!");
    }

    /**
     * 刷出请求到下个RequestProcessor
     *
     * @param toFlush
     * @throws IOException
     * @throws RequestProcessorException
     */
    private void flush(LinkedList<Request> toFlush)
        throws IOException, RequestProcessorException
    {
        if (toFlush.isEmpty())
            return;

        // 刷出TxnLog到磁盘
        zks.getZKDatabase().commit();
        while (!toFlush.isEmpty()) {
            Request i = toFlush.remove();
            if (nextProcessor != null) {
                nextProcessor.processRequest(i);
            }
        }
        if (nextProcessor != null && nextProcessor instanceof Flushable) {
            ((Flushable)nextProcessor).flush();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");
        // 添加中止请求到队列
        // 因为这个线程是依赖queuedRequests的take方法进行阻塞，add方法可以解除阻塞，如果判断到request是requestOfDeath，那么线程就中止
        queuedRequests.add(requestOfDeath);
        try {
            if(running){
                // 等待该线程到死
                this.join();
            }
            // 需要刷新日志到磁盘
            if (!toFlush.isEmpty()) {
                flush(toFlush);
            }
        } catch(InterruptedException e) {
            LOG.warn("Interrupted while wating for " + this + " to finish");
        } catch (IOException e) {
            LOG.warn("Got IO exception during shutdown");
        } catch (RequestProcessorException e) {
            LOG.warn("Got request processor exception during shutdown");
        }
        // 关闭下一个processor
        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

    public void processRequest(Request request) {
        // request.addRQRec(">sync");
        queuedRequests.add(request);
    }

}
