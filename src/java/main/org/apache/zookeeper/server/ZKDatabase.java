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

import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.WatcherType;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.DataTree.ProcessTxnResult;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.PlayBackListener;
import org.apache.zookeeper.server.persistence.TxnLog.TxnIterator;
import org.apache.zookeeper.server.quorum.Leader;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPacket;
import org.apache.zookeeper.server.quorum.flexible.QuorumVerifier;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * 维护zookeeper server 状态的内存数据库，包括sessions，dataTree和committed logs。
 * 读取日志和快照后启动
 *
 * This class maintains the in memory database of zookeeper
 * server states that includes the sessions, datatree and the
 * committed logs. It is booted up  after reading the logs
 * and snapshots from the disk.
 */
public class ZKDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(ZKDatabase.class);

    /**
     * make sure on a clear you take care of
     * all these members.
     */
    protected DataTree dataTree;
    // sessionId与timeout的映射
    // 记录了所有存活的session
    // key:clientId,value:sessionTimeout
    protected ConcurrentHashMap<Long, Integer> sessionsWithTimeouts;
    protected FileTxnSnapLog snapLog;
    protected long minCommittedLog, maxCommittedLog;
    
    /**
     * Default value is to use snapshot if txnlog size exceeds 1/3 the size of snapshot
     */
    public static final String SNAPSHOT_SIZE_FACTOR = "zookeeper.snapshotSizeFactor";
    private double snapshotSizeFactor = 0.33;
    
    public static final int commitLogCount = 500;
    protected static int commitLogBuffer = 700;
    protected LinkedList<Proposal> committedLog = new LinkedList<Proposal>();
    // TODO
    protected ReentrantReadWriteLock logLock = new ReentrantReadWriteLock();
    volatile private boolean initialized = false;

    /**
     * the filetxnsnaplog that this zk database
     * maps to. There is a one to one relationship
     * between a filetxnsnaplog and zkdatabase.
     * @param snapLog the FileTxnSnapLog mapping this zkdatabase
     */
    public ZKDatabase(FileTxnSnapLog snapLog) {
        dataTree = new DataTree();
        sessionsWithTimeouts = new ConcurrentHashMap<Long, Integer>();
        this.snapLog = snapLog;
    }

    /**
     * checks to see if the zk database has been
     * initialized or not.
     * @return true if zk database is initialized and false if not
     */
    public boolean isInitialized() {
        return initialized;
    }

    /**
     * clear the zkdatabase.
     * Note to developers - be careful to see that
     * the clear method does clear out all the
     * data structures in zkdatabase.
     */
    public void clear() {
        minCommittedLog = 0;
        maxCommittedLog = 0;
        /* to be safe we just create a new
         * datatree.
         */
        dataTree = new DataTree();
        sessionsWithTimeouts.clear();
        WriteLock lock = logLock.writeLock();
        try {
            lock.lock();
            committedLog.clear();
        } finally {
            lock.unlock();
        }
        initialized = false;
    }

    /**
     * the datatree for this zkdatabase
     * @return the datatree for this zkdatabase
     */
    public DataTree getDataTree() {
        return this.dataTree;
    }

    /**
     * the committed log for this zk database
     * @return the committed log for this zkdatabase
     */
    public long getmaxCommittedLog() {
        return maxCommittedLog;
    }


    /**
     * the minimum committed transaction log
     * available in memory
     * @return the minimum committed transaction
     * log available in memory
     */
    public long getminCommittedLog() {
        return minCommittedLog;
    }
    /**
     * Get the lock that controls the committedLog. If you want to get the pointer to the committedLog, you need
     * to use this lock to acquire a read lock before calling getCommittedLog()
     * @return the lock that controls the committed log
     */
    public ReentrantReadWriteLock getLogLock() {
        return logLock;
    }


    public synchronized List<Proposal> getCommittedLog() {
        ReadLock rl = logLock.readLock();
        // only make a copy if this thread isn't already holding a lock
        if(logLock.getReadHoldCount() <=0) {
            try {
                rl.lock();
                return new LinkedList<Proposal>(this.committedLog);
            } finally {
                rl.unlock();
            }
        }
        return this.committedLog;
    }

    /**
     * get the last processed zxid from a datatree
     * @return the last processed zxid of a datatree
     */
    public long getDataTreeLastProcessedZxid() {
        return dataTree.lastProcessedZxid;
    }

    /**
     * return the sessions in the datatree
     * @return the data tree sessions
     */
    public Collection<Long> getSessions() {
        return dataTree.getSessions();
    }

    /**
     * get sessions with timeouts
     * @return the hashmap of sessions with timeouts
     */
    public ConcurrentHashMap<Long, Integer> getSessionWithTimeOuts() {
        return sessionsWithTimeouts;
    }


    /**
     * load the database from the disk onto memory and also add
     * the transactions to the committedlog in memory.
     * @return the last valid zxid on disk
     * @throws IOException
     */
    public long loadDataBase() throws IOException {
        PlayBackListener listener=new PlayBackListener(){
            public void onTxnLoaded(TxnHeader hdr,Record txn){
                Request r = new Request(0, hdr.getCxid(),hdr.getType(), hdr, txn, hdr.getZxid());
                addCommittedProposal(r);
            }
        };
        // 快照存储
        long zxid = snapLog.restore(dataTree,sessionsWithTimeouts,listener);
        initialized = true;
        return zxid;
    }

    /**
     * maintains a list of last <i>committedLog</i>
     *  or so committed requests. This is used for
     * fast follower synchronization.
     * @param request committed request
     */
    public void addCommittedProposal(Request request) {
        WriteLock wl = logLock.writeLock();
        try {
            wl.lock();
            if (committedLog.size() > commitLogCount) {
                committedLog.removeFirst();
                minCommittedLog = committedLog.getFirst().packet.getZxid();
            }
            if (committedLog.isEmpty()) {
                minCommittedLog = request.zxid;
                maxCommittedLog = request.zxid;
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            BinaryOutputArchive boa = BinaryOutputArchive.getArchive(baos);
            try {
                request.getHdr().serialize(boa, "hdr");
                if (request.getTxn() != null) {
                    request.getTxn().serialize(boa, "txn");
                }
                baos.close();
            } catch (IOException e) {
                LOG.error("This really should be impossible", e);
            }
            // 发起提议数据包,会保存committedLog，并记录当前包的zxid到maxCommittedLog
            // 这个提议数据zxid不仅保存到内存并已经持久化到磁盘
            QuorumPacket pp = new QuorumPacket(Leader.PROPOSAL, request.zxid,
                    baos.toByteArray(), null);
            Proposal p = new Proposal();
            p.packet = pp;
            p.request = request;
            committedLog.add(p);
            maxCommittedLog = p.packet.getZxid();
        } finally {
            wl.unlock();
        }
    }
    
    public double getSnapshotSizeFactor() {
        return snapshotSizeFactor;
    }

    /**
     * 读取文件的字节数大小
     * @return
     */
    public long calculateTxnLogSizeLimit() {
        long snapSize = 0;
        try {
            snapSize = snapLog.findMostRecentSnapshot().length();
        } catch (IOException e) {
            LOG.error("Unable to get size of most recent snapshot");
        }
        return (long) (snapSize * snapshotSizeFactor);
    }

    /**
     * Get proposals from txnlog. Only packet part of proposal is populated.
     *
     * @param startZxid the starting zxid of the proposal
     * @param sizeLimit maximum on-disk size of txnlog to fetch
     *                  0 is unlimited, negative value means disable.
     * @return list of proposal (request part of each proposal is null)
     */
    public Iterator<Proposal> getProposalsFromTxnLog(long startZxid,
                                                     long sizeLimit) {
        if (sizeLimit < 0) {
            LOG.debug("Negative size limit - retrieving proposal via txnlog is disabled");
            return TxnLogProposalIterator.EMPTY_ITERATOR;
        }

        TxnIterator itr = null;
        try {

            itr = snapLog.readTxnLog(startZxid, false);

            // If we cannot guarantee that this is strictly the starting txn
            // after a given zxid, we should fail.
            if ((itr.getHeader() != null)
                    && (itr.getHeader().getZxid() > startZxid)) {
                LOG.warn("Unable to find proposals from txnlog for zxid: "
                        + startZxid);
                itr.close();
                return TxnLogProposalIterator.EMPTY_ITERATOR;
            }

            if (sizeLimit > 0) {
                long txnSize = itr.getStorageSize();
                if (txnSize > sizeLimit) {
                    LOG.info("Txnlog size: " + txnSize + " exceeds sizeLimit: "
                            + sizeLimit);
                    itr.close();
                    return TxnLogProposalIterator.EMPTY_ITERATOR;
                }
            }
        } catch (IOException e) {
            LOG.error("Unable to read txnlog from disk", e);
            try {
                if (itr != null) {
                    itr.close();
                }
            } catch (IOException ioe) {
                LOG.warn("Error closing file iterator", ioe);
            }
            return TxnLogProposalIterator.EMPTY_ITERATOR;
        }
        return new TxnLogProposalIterator(itr);
    }

    public List<ACL> aclForNode(DataNode n) {
        return dataTree.getACL(n);
    }
    /**
     * remove a cnxn from the datatree
     * @param cnxn the cnxn to remove from the datatree
     */
    // 从内存中的dataTree中移除serverCnxn
    public void removeCnxn(ServerCnxn cnxn) {
        dataTree.removeCnxn(cnxn);
    }

    /**
     * kill a given session in the datatree
     * @param sessionId the session id to be killed
     * @param zxid the zxid of kill session transaction
     */
    public void killSession(long sessionId, long zxid) {
        dataTree.killSession(sessionId, zxid);
    }

    /**
     * write a text dump of all the ephemerals in the datatree
     * @param pwriter the output to write to
     */
    public void dumpEphemerals(PrintWriter pwriter) {
        dataTree.dumpEphemerals(pwriter);
    }

    public Map<Long, Set<String>> getEphemerals() {
        return dataTree.getEphemerals();
    }

    /**
     * the node count of the datatree
     * @return the node count of datatree
     */
    public int getNodeCount() {
        return dataTree.getNodeCount();
    }

    /**
     * the paths for  ephemeral session id
     * @param sessionId the session id for which paths match to
     * @return the paths for a session id
     */
    public Set<String> getEphemerals(long sessionId) {
        return dataTree.getEphemerals(sessionId);
    }

    /**
     * the last processed zxid in the datatree
     * @param zxid the last processed zxid in the datatree
     */
    public void setlastProcessedZxid(long zxid) {
        dataTree.lastProcessedZxid = zxid;
    }

    /**
     * the process txn on the data
     * @param hdr the txnheader for the txn
     * @param txn the transaction that needs to be processed
     * @return the result of processing the transaction on this
     * datatree/zkdatabase
     */
    public ProcessTxnResult processTxn(TxnHeader hdr, Record txn) {
        return dataTree.processTxn(hdr, txn);
    }

    /**
     * stat the path
     * @param path the path for which stat is to be done
     * @param serverCnxn the servercnxn attached to this request
     * @return the stat of this node
     * @throws KeeperException.NoNodeException
     */
    public Stat statNode(String path, ServerCnxn serverCnxn) throws KeeperException.NoNodeException {
        return dataTree.statNode(path, serverCnxn);
    }

    /**
     * get the datanode for this path
     * @param path the path to lookup
     * @return the datanode for getting the path
     */
    public DataNode getNode(String path) {
      return dataTree.getNode(path);
    }

    /**
     * get data and stat for a path
     * @param path the path being queried
     * @param stat the stat for this path
     * @param watcher the watcher function
     * @return
     * @throws KeeperException.NoNodeException
     */
    public byte[] getData(String path, Stat stat, Watcher watcher)
    throws KeeperException.NoNodeException {
        return dataTree.getData(path, stat, watcher);
    }

    /**
     * set watches on the datatree
     * @param relativeZxid the relative zxid that client has seen
     * @param dataWatches the data watches the client wants to reset
     * @param existWatches the exists watches the client wants to reset
     * @param childWatches the child watches the client wants to reset
     * @param watcher the watcher function
     */
    public void setWatches(long relativeZxid, List<String> dataWatches,
            List<String> existWatches, List<String> childWatches, Watcher watcher) {
        dataTree.setWatches(relativeZxid, dataWatches, existWatches, childWatches, watcher);
    }

    /**
     * get acl for a path
     * @param path the path to query for acl
     * @param stat the stat for the node
     * @return the acl list for this path
     * @throws NoNodeException
     */
    public List<ACL> getACL(String path, Stat stat) throws NoNodeException {
        return dataTree.getACL(path, stat);
    }

    /**
     * get children list for this path
     * @param path the path of the node
     * @param stat the stat of the node
     * @param watcher the watcher function for this path
     * @return the list of children for this path
     * @throws KeeperException.NoNodeException
     */
    public List<String> getChildren(String path, Stat stat, Watcher watcher)
    throws KeeperException.NoNodeException {
        return dataTree.getChildren(path, stat, watcher);
    }

    /**
     * check if the path is special or not
     * @param path the input path
     * @return true if path is special and false if not
     */
    public boolean isSpecialPath(String path) {
        return dataTree.isSpecialPath(path);
    }

    /**
     * get the acl size of the datatree
     * @return the acl size of the datatree
     */
    public int getAclSize() {
        return dataTree.aclCacheSize();
    }

    /**
     * Truncate the ZKDatabase to the specified zxid
     * @param zxid the zxid to truncate zk database to
     * @return true if the truncate is successful and false if not
     * @throws IOException
     */
    // truncate的时候清空内存数据，重新加载
    public boolean truncateLog(long zxid) throws IOException {
        clear();

        // truncate the log
        boolean truncated = snapLog.truncateLog(zxid);

        if (!truncated) {
            return false;
        }

        loadDataBase();
        return true;
    }

    /**
     * deserialize a snapshot from an input archive
     * @param ia the input archive you want to deserialize from
     * @throws IOException
     */
    // 清空内存数据，从流中反序列化数据
    public void deserializeSnapshot(InputArchive ia) throws IOException {
        clear();
        SerializeUtils.deserializeSnapshot(getDataTree(),ia,getSessionWithTimeOuts());
        initialized = true;
    }

    /**
     * serialize the snapshot
     * @param oa the output archive to which the snapshot needs to be serialized
     * @throws IOException
     * @throws InterruptedException
     */
    public void serializeSnapshot(OutputArchive oa) throws IOException,
    InterruptedException {
        SerializeUtils.serializeSnapshot(getDataTree(), oa, getSessionWithTimeOuts());
    }

    /**
     * append to the underlying transaction log
     * @param si the request to append
     * @return true if the append was succesfull and false if not
     */
    // 写出到data目录下
    public boolean append(Request si) throws IOException {
        return this.snapLog.append(si);
    }

    /**
     * roll the underlying log
     */
    // 刷出data日志文件，并关闭文件流
    public void rollLog() throws IOException {
        this.snapLog.rollLog();
    }

    /**
     * commit to the underlying transaction log
     * @throws IOException
     */
    // 刷出data日志文件数据到磁盘
    public void commit() throws IOException {
        this.snapLog.commit();
    }

    /**
     * close this database. free the resources
     * @throws IOException
     */
    public void close() throws IOException {
        this.snapLog.close();
    }

    public synchronized void initConfigInZKDatabase(QuorumVerifier qv) {
        if (qv == null) return; // only happens during tests
        try {
            if (this.dataTree.getNode(ZooDefs.CONFIG_NODE) == null) {
                // should only happen during upgrade
                LOG.warn("configuration znode missing (hould only happen during upgrade), creating the node");
                this.dataTree.addConfigNode();
            }
            this.dataTree.setData(ZooDefs.CONFIG_NODE, qv.toString().getBytes(), -1, qv.getVersion(), Time.currentWallTime());
        } catch (NoNodeException e) {
            System.out.println("configuration node missing - should not happen");
        }
    }
 
    /**
     * Use for unit testing, so we can turn this feature on/off
     * @param snapshotSizeFactor Set to minus value to turn this off.
     */
    public void setSnapshotSizeFactor(double snapshotSizeFactor) {
        this.snapshotSizeFactor = snapshotSizeFactor;
    }

    /**
     * Check whether the given watcher exists in datatree
     *
     * @param path
     *            node to check watcher existence
     * @param type
     *            type of watcher
     * @param watcher
     *            watcher function
     */
    public boolean containsWatcher(String path, WatcherType type, Watcher watcher) {
        return dataTree.containsWatcher(path, type, watcher);
    }

    /**
     * Remove watch from the datatree
     * 
     * @param path
     *            node to remove watches from
     * @param type
     *            type of watcher to remove
     * @param watcher
     *            watcher function to remove
     */
    public boolean removeWatch(String path, WatcherType type, Watcher watcher) {
        return dataTree.removeWatch(path, type, watcher);
    }
}
