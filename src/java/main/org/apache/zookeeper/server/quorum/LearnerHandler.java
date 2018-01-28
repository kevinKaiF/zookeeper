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

package org.apache.zookeeper.server.quorum;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;

import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.Record;
import org.apache.zookeeper.KeeperException.SessionExpiredException;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.TxnLogProposalIterator;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.apache.zookeeper.txn.TxnHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 */
// 处理选举后的follower,observer的请求
public class LearnerHandler extends ZooKeeperThread {
    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;

    public Socket getSocket() {
        return sock;
    }

    final Leader leader;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;
    
    /**
     * ZooKeeper server identifier of this learner
     */
    // 是接收到的follower,observer的serverId
    protected long sid = 0;

    long getSid(){
        return sid;
    }

    protected int version = 0x1;

    int getVersion() {
    	return version;
    }

    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets =
        new LinkedBlockingQueue<QuorumPacket>();

    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {
        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
             if (currentZxid == zxid) {
                 currentTime = nextTime;
                 currentZxid = nextZxid;
                 nextTime = 0;
                 nextZxid = 0;
             } else if (nextZxid == zxid) {
                 LOG.warn("ACK for " + zxid + " received before ACK for " + currentZxid + "!!!!");
                 nextTime = 0;
                 nextZxid = 0;
             }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < (leader.self.tickTime * leader.self.syncLimit));
            }
        }
    };

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private BufferedOutputStream bufferedOutput;
    
    /**
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;

    /**
     * For testing purpose, force leader to use snapshot to sync with followers
     */
    public static final String FORCE_SNAP_SYNC = "zookeeper.forceSnapshotSync";
    private boolean forceSnapSync = false;

    /**
     * Keep track of whether we need to queue TRUNC or DIFF into packet queue
     * that we are going to blast it to the learner
     */
    // 如果为false,说明需要TRUNC,DIFF
    // 否则不需要
    private boolean needOpPacket = true;
    
    /**
     * Last zxid sent to the learner as part of synchronization
     */
    private long leaderLastZxid;

    // socket是follower,observer
    LearnerHandler(Socket sock, Leader leader) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.leader = leader;
        // 注册到leader的leaderHandlers
        leader.addLearnerHandler(this);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType  learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }
                oa.writeRecord(p, "packet");
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at " + this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch(IOException ie) {
                        LOG.warn("Error closing socket for handler " + this, ie);
                    }
                }
                break;
            }
        }
    }

    static public String packetToString(QuorumPacket p) {
        String type;
        String mess = null;

        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            TxnHeader hdr = new TxnHeader();
            try {
                SerializeUtils.deserializeTxn(p.getData(), hdr);
                // mess = "transaction: " + txn.toString();
            } catch (IOException e) {
                LOG.warn("Unexpected exception",e);
            }
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        case Leader.DIFF:
            type = "DIFF";
            break;
        case Leader.TRUNC:
            type = "TRUNC";
            break;
        case Leader.SNAP:
            type = "SNAP";
            break;
        case Leader.ACKEPOCH:
            type = "ACKEPOCH";
            break;
        case Leader.SYNC:
            type = "SYNC";
            break;
        case Leader.INFORM:
            type = "INFORM";
            break;
        case Leader.COMMITANDACTIVATE:
            type = "COMMITANDACTIVATE";
            break;
        case Leader.INFORMANDACTIVATE:
            type = "INFORMANDACTIVATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            // initLimit是选举完毕后，开始统一集群数据，leader和follower,observer设置的read超时时间
            // syncLimit是统一集群数据完毕之后，leader和follower,observer设置的read超时时间
            tickOfNextAckDeadline = leader.self.tick.get()
                    + leader.self.initLimit + leader.self.syncLimit;
            // 读取follower,observer的请求数据
            ia = BinaryInputArchive.getArchive(new BufferedInputStream(sock
                    .getInputStream()));
            // 写出follower,observer的响应数据
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            QuorumPacket qp = new QuorumPacket();
            ia.readRecord(qp, "packet");
            // 如果不是followerInfo,observerInfo就return
            if(qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO){
            	LOG.error("First packet " + qp.toString()
                        + " is not FOLLOWERINFO or OBSERVERINFO!");
                return;
            }
 
            byte learnerInfoData[] = qp.getData();
            if (learnerInfoData != null) {
                ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                // long 8个字节
                if (learnerInfoData.length >= 8) {
                    // 对方的serverId
                    this.sid = bbsid.getLong();
                }
                // sid long 8个字节
                // version int 4个字节
                // 更新version
                // 这里很关键，因为version的原始值是0x1,只有接收到follower,observer的请求才会变为0x10000
                // 如果变更了，才接收到了客户端的ack
                if (learnerInfoData.length >= 12) {
                    this.version = bbsid.getInt(); // protocolVersion
                }
                if (learnerInfoData.length >= 20) {
                    long configVersion = bbsid.getLong();
                    if (configVersion > leader.self.getQuorumVerifier().getVersion()) {
                        throw new IOException("Follower is ahead of the leader (has a later activated configuration)");                     
                    }
                }
            } else {
                // 如果没有数据，则followerCounter递增,并赋值给当前socket的serverId
                this.sid = leader.followerCounter.getAndDecrement();
            }

            // 对方的serverId在集群中
            if (leader.self.getView().containsKey(this.sid)) {
                LOG.info("Follower sid: " + this.sid + " : info : "
                        + leader.self.getView().get(this.sid).toString());
            } else {
                LOG.info("Follower sid: " + this.sid + " not in the current config " + Long.toHexString(leader.self.getQuorumVerifier().getVersion()));
            }
                        
            if (qp.getType() == Leader.OBSERVERINFO) {
                  learnerType = LearnerType.OBSERVER;
            }

            // 对方的epoch
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

            long peerLastZxid;
            StateSummary ss = null;
            long zxid = qp.getZxid();
            // 更新leader的代数
            // 这里会多线程阻塞等待所有的leader,follower,observer有个统一的newEpoch
            // 多线程阻塞等待follower,observer的注册
            /**
             * 对应{@link Learner#registerWithLeader(int)}的LearnerInfo
             */
            long newEpoch = leader.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            // 设置新的zxid
            long newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);
            // 兼容老版本zk?
            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                /**
                 * 对应{@link Learner#registerWithLeader(int)}的Leader.ACKEPOCH
                 */
                // fake the message
                // 多线程阻塞等待集群中所有observer,follower,observer的ack
                leader.waitForEpochAck(this.getSid(), ss);
            } else {
                byte ver[] = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                // 选举结束，向对方发送leader信息
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, newLeaderZxid, ver, null);
                oa.writeRecord(newEpochPacket, "packet");
                bufferedOutput.flush();
                // 再读取对方的ACKEPOCH信息
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                /**
                 * 对应{@link Learner#registerWithLeader(int)}的Leader.ACKEPOCH
                 */
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    LOG.error(ackEpochPacket.toString()
                            + " is not ACKEPOCH");
                    return;
				}
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                // 多线程阻塞等待集群中所有observer,follower,observer的ACKEPOCH
                leader.waitForEpochAck(this.getSid(), ss);
            }

            // 获取follower,observer的最新的zxid
            peerLastZxid = ss.getLastZxid();
           
            // Take any necessary action if we need to send TRUNC or DIFF
            // startForwarding() will be called in all cases
            boolean needSnap = syncFollower(peerLastZxid, leader.zk.getZKDatabase(), leader);
            
            LOG.debug("Sending NEWLEADER message to " + sid);
            // the version of this quorumVerifier will be set by leader.lead() in case
            // the leader is just being established. waitForEpochAck makes sure that readyToStart is true if
            // we got here, so the version was set
            // 兼容老版本？
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                        newLeaderZxid, null, null);
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                // 将NEWLEADER以及集群数据发送给follower,observer
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER,
                        newLeaderZxid, leader.self.getLastSeenQuorumVerifier()
                                .toString().getBytes(), null);
                queuedPackets.add(newLeaderQP);
            }
            bufferedOutput.flush();

            /* if we are not truncating or sending a diff just send a snapshot */
            // 如果zxid需要snapshot比对
            if (needSnap) {
                // 是否是观察者
                boolean exemptFromThrottle = getLearnerType() != LearnerType.OBSERVER;
                LearnerSnapshot snapshot = 
                        leader.getLearnerSnapshotThrottler().beginSnapshot(exemptFromThrottle);
                try {
                    // 获取leader zkDb最新processed的zxid发送给客户端
                    long zxidToSend = leader.zk.getZKDatabase().getDataTreeLastProcessedZxid();
                    oa.writeRecord(new QuorumPacket(Leader.SNAP, zxidToSend, null, null), "packet");
                    bufferedOutput.flush();

                    LOG.info("Sending snapshot last zxid of peer is 0x{}, zxid of leader is 0x{}, "
                            + "send zxid of db as 0x{}, {} concurrent snapshots, " 
                            + "snapshot was {} from throttle",
                            Long.toHexString(peerLastZxid), 
                            Long.toHexString(leaderLastZxid),
                            Long.toHexString(zxidToSend), 
                            snapshot.getConcurrentSnapshotNumber(),
                            snapshot.isEssential() ? "exempt" : "not exempt");
                    // Dump data to peer
                    leader.zk.getZKDatabase().serializeSnapshot(oa);
                    oa.writeString("BenWasHere", "signature");
                    bufferedOutput.flush();
                } finally {
                    snapshot.close();
                }
            }

            // 先发送SNAP
            // 然后发送计入队列的数据，都是持久化到日志的数据

            // Start thread that blast packets in the queue to learner
            startSendingPackets();
            
            /*
             * Have to wait for the first ACK, wait until
             * the leader is ready, and only then we can
             * start processing messages.
             */
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");
            /**
             * 读取follower,observer的ack，如果不是ACK,那么leaderHandler就死掉
             * 对应{@link Learner#syncWithLeader(long)}
              */
            if(qp.getType() != Leader.ACK){
                LOG.error("Next packet was supposed to be an ACK,"
                    + " but received packet: {}", packetToString(qp));
                return;
            }

            if(LOG.isDebugEnabled()){
            	LOG.debug("Received NEWLEADER-ACK message from " + sid);   
            }
            // 多线程等待follower的NEWLEADERACK
            /**
             * 对应{@link Learner#syncWithLeader(long)}的Leader.NEWLEADER的ACK
             */
            leader.waitForNewLeaderAck(getSid(), qp.getZxid(), getLearnerType());

            syncLimitCheck.start();
            
            // now that the ack has been processed expect the syncLimit
            // 由于leader与follower,observer统一zxid完毕，则需要重新设置syncLimit来进行集群正常的通信
            // 重新设置timeout
            sock.setSoTimeout(leader.self.tickTime * leader.self.syncLimit);

            /*
             * Wait until leader starts up
             */
            // 阻塞等待leader的zk启动
            synchronized(leader.zk){
                while(!leader.zk.isRunning() && !this.isInterrupted()){
                    leader.zk.wait(20);
                }
            }
            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            //
            LOG.debug("Sending UPTODATE message to " + sid);
            // 给follower,observer发送UPTODATE
            // 表示会议结束了，follower,observer可以响应客户端了
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));

            /**
             * 开始处理来自follower,observer正常的请求数据
             */
            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                // 读取leader的tick，加上syncLimit来计算下次的时间戳
                tickOfNextAckDeadline = leader.self.tick.get() + leader.self.syncLimit;


                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                case Leader.ACK:
                    // 接收到follower的ack
                    if (this.learnerType == LearnerType.OBSERVER) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received ACK from Observer  " + this.sid);
                        }
                    }
                    syncLimitCheck.updateAck(qp.getZxid());
                    leader.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                case Leader.PING:
                    // 读取follower,observer所有的sessionId,sessionTimeout
                    // 为什么会发送sessionTimeout呢？
                    // 因为sessionTimeout呢在集群中的每个节点都设置的session都不同
                    /**
                     * 对应{@link Learner#ping(QuorumPacket)}
                     */
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp
                            .getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        // 读取sessionId
                        long sess = dis.readLong();
                        // 读取sessionTimeout
                        int to = dis.readInt();
                        // 更新session的过期时间戳
                        leader.zk.touch(sess, to);
                    }
                    break;
                case Leader.REVALIDATE:
                    bis = new ByteArrayInputStream(qp.getData());
                    dis = new DataInputStream(bis);
                    long id = dis.readLong();
                    int to = dis.readInt();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    DataOutputStream dos = new DataOutputStream(bos);
                    dos.writeLong(id);
                    boolean valid = leader.zk.checkIfValidGlobalSession(id, to);
                    if (valid) {
                        try {
                            //set the session owner
                            // as the follower that
                            // owns the session
                            leader.zk.setOwner(id, this);
                        } catch (SessionExpiredException e) {
                            LOG.error("Somehow session " + Long.toHexString(id) + " expired right after being renewed! (impossible)", e);
                        }
                    }
                    if (LOG.isTraceEnabled()) {
                        ZooTrace.logTraceMessage(LOG,
                                                 ZooTrace.SESSION_TRACE_MASK,
                                                 "Session 0x" + Long.toHexString(id)
                                                 + " is valid: "+ valid);
                    }
                    dos.writeBoolean(valid);
                    qp.setData(bos.toByteArray());
                    queuedPackets.add(qp);
                    break;
                case Leader.REQUEST:
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    if(type == OpCode.sync){
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    // 接收到follower,observer的REQUEST类型请求，交给leader的LeaderZookeeperServer处理
                    // 直接commit到prepRequestProcessor进行处理
                    leader.zk.submitLearnerRequest(si);
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock "
                        + "still open", e);
            	//close the socket to make sure the
            	//other side can see it being close
            	try {
            		sock.close();
            	} catch(IOException ie) {
            		// do nothing
            	}
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception causing shutdown", e);
        } catch (SnapshotThrottleException e) {
            LOG.error("too many concurrent snapshots: " + e);
        } finally {
            LOG.warn("******* GOODBYE "
                    + (sock != null ? sock.getRemoteSocketAddress() : "<null>")
                    + " ********");
            shutdown();
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the follower
     */
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName(
                            "Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption " + e.getMessage());
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * Determine if we need to sync with follower using DIFF/TRUNC/SNAP
     * and setup follower to receive packets from commit processor
     *
     * @param peerLastZxid  follower,observer的zxid
     * @param db            leader的ZKDatabase
     * @param leader
     * @return true if snapshot transfer is needed.
     */
    public boolean syncFollower(long peerLastZxid, ZKDatabase db, Leader leader) {
        /*
         * When leader election is completed, the leader will set its
         * lastProcessedZxid to be (epoch < 32). There will be no txn associated
         * with this zxid.
         *
         * The learner will set its lastProcessedZxid to the same value if
         * it get DIFF or SNAP from the leader. If the same learner come
         * back to sync with leader using this zxid, we will never find this
         * zxid in our history. In this case, we will ignore TRUNC logic and
         * always send DIFF if we have old enough history
         */
        // peerLastZxid是follower,observer的zxid
        // 判断follower,observer的zxid是否是0x00000000,即判断低32位的zxid是否是0x00000000
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // Keep track of the latest zxid which already queued
        // follower,observer的zxid
        long currentZxid = peerLastZxid;
        boolean needSnap = true;
        boolean txnLogSyncEnabled = (db.getSnapshotSizeFactor() >= 0);
        ReentrantReadWriteLock lock = db.getLogLock();
        ReadLock rl = lock.readLock();
        try {
            rl.lock();
            // zk 中最大的zxid
            long maxCommittedLog = db.getmaxCommittedLog();
            long minCommittedLog = db.getminCommittedLog();
            // 当前leader最新的zxid
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();

            LOG.info("Synchronizing with Follower sid: {} maxCommittedLog=0x{}"
                    + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                    + " peerLastZxid=0x{}", getSid(),
                    Long.toHexString(maxCommittedLog),
                    Long.toHexString(minCommittedLog),
                    Long.toHexString(lastProcessedZxid),
                    Long.toHexString(peerLastZxid));

            // 如果leader的提议列表是空的，将重置最大最小的committed的zxid
            if (db.getCommittedLog().isEmpty()) {
                /*
                 * It is possible that commitedLog is empty. In that case
                 * setting these value to the latest txn in leader db
                 * will reduce the case that we need to handle
                 *
                 * Here is how each case handle by the if block below
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 */
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }

            /*
             * Here are the cases that we want to handle
             *
             * 1. Force sending snapshot (for testing purpose)
             * 2. Peer and leader is already sync, send empty diff
             * 3. Follower has txn that we haven't seen. This may be old leader
             *    so we need to send TRUNC. However, if peer has newEpochZxid,
             *    we cannot send TRUC since the follower has no txnlog
             * 4. Follower is within committedLog range or already in-sync.
             *    We may need to send DIFF or TRUNC depending on follower's zxid
             *    We always send empty DIFF if follower is already in-sync
             * 5. Follower missed the committedLog. We will try to use on-disk
             *    txnlog + committedLog to sync with follower. If that fail,
             *    we will send snapshot
             */

            if (forceSnapSync) {
                // Force leader to use snapshot to sync with follower
                LOG.warn("Forcing snapshot sync - should not see this in production");
            } else if (lastProcessedZxid == peerLastZxid) {
                // 说明follower 已经和 leader 同步好了
                // Follower is already sync with us, send empty diff
                LOG.info("Sending DIFF zxid=0x" + Long.toHexString(peerLastZxid) +
                         " for peer sid: " +  getSid());
                queueOpPacket(Leader.DIFF, peerLastZxid);
                needOpPacket = false;
                needSnap = false;
            } else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                // 如果follower,observer的zxid比leader的maxCommittedLog还要大，且follower的zxid不是新的，0
                // 则leader命令follower做日志的截断
                // Newer than commitedLog, send trunc and done
                LOG.debug("Sending TRUNC to follower zxidToSend=0x" +
                          Long.toHexString(maxCommittedLog) +
                          " for peer sid:" +  getSid());
                // 发送截断位置
                queueOpPacket(Leader.TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            } else if ((maxCommittedLog >= peerLastZxid)
                    && (minCommittedLog <= peerLastZxid)) {
                // 如果follower,observer的zxid，在leader的[minCommittedLog, maxCommittedLog]之间
                // Follower is within commitLog range
                LOG.info("Using committedLog for peer sid: " +  getSid());
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                currentZxid = queueCommittedProposals(itr, peerLastZxid,
                                                     null, maxCommittedLog);
                needSnap = false;
            } else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {
                // 如果follower,observer的zxid比minCommittedLog还要小,启用日志同步？
                // Use txnlog and committedLog to sync

                // Calculate sizeLimit that we allow to retrieve txnlog from disk
                // 计算需要同步日志字节数
                long sizeLimit = db.calculateTxnLogSizeLimit();
                // This method can return empty iterator if the requested zxid
                // is older than on-disk txnlog
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(
                        peerLastZxid, sizeLimit);
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: " +  getSid());
                    // 从leader的日志中找到小于minCommittedLog的最大zxid
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid,
                                                         minCommittedLog, maxCommittedLog);

                    LOG.debug("Queueing committedLog 0x" + Long.toHexString(currentZxid));
                    Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                    // 从committedLog中获取最接近currentZxid的zxid
                    currentZxid = queueCommittedProposals(committedLogItr, currentZxid,
                                                         null, maxCommittedLog);
                    needSnap = false;
                }
                // closing the resources
                if (txnLogItr instanceof TxnLogProposalIterator) {
                    TxnLogProposalIterator txnProposalItr = (TxnLogProposalIterator) txnLogItr;
                    txnProposalItr.close();
                }
            } else {
                LOG.warn("Unhandled scenario for peer sid: " +  getSid());
            }
            LOG.debug("Start forwarding 0x" + Long.toHexString(currentZxid) +
                      " for peer sid: " +  getSid());
            // 返回leader最新的zxid
            leaderLastZxid = leader.startForwarding(this, currentZxid);
        } finally {
            rl.unlock();
        }

        // 如果follower,observer的zxid不能从持久化的日志中判断，则需要leader的快照日志
        if (needOpPacket && !needSnap) {
            // This should never happen, but we should fall back to sending
            // snapshot just in case.
            LOG.error("Unhandled scenario for peer sid: " +  getSid() +
                     " fall back to use snapshot");
            needSnap = true;
        }

        return needSnap;
    }

    /**
     * Queue committed proposals into packet queue. The range of packets which
     * is going to be queued are (peerLaxtZxid, maxZxid]
     *
     * @param itr  iterator point to the proposals
     * @param peerLastZxid  last zxid seen by the follower
     * @param maxZxid  max zxid of the proposal to queue, null if no limit
     * @param lastCommitedZxid when sending diff, we need to send lastCommitedZxid
     *        on the leader to follow Zab 1.0 protocol.
     * @return last zxid of the queued proposal
     */
    // 将leader的committedLog的zxid数据发送给follower,observer
    protected long queueCommittedProposals(Iterator<Proposal> itr,
            long peerLastZxid, Long maxZxid, Long lastCommitedZxid) {
        // follower,observer的zxid是否是最新的0x00000000
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // follower,observer的zxid
        long queuedZxid = peerLastZxid;
        // as we look through proposals, this variable keeps track of previous
        // proposal Id.
        long prevProposalZxid = -1;
        // 遍历leader的提议
        while (itr.hasNext()) {
            Proposal propose = itr.next();

            long packetZxid = propose.packet.getZxid();
            // abort if we hit the limit
            // 如果committedLog的zxid比maxZxid还要大，直接break
            // 说明有新的数据committedLog产生了，不能再比较了
            if ((maxZxid != null) && (packetZxid > maxZxid)) {
                break;
            }

            // skip the proposals the peer already has
            // 记录最接近follower,observer的zxid
            if (packetZxid < peerLastZxid) {
                prevProposalZxid = packetZxid;
                continue;
            }

            // If we are sending the first packet, figure out whether to trunc
            // or diff
            // 如果没有TRUNC,DIFF
            if (needOpPacket) {

                // Send diff when we see the follower's zxid in our history
                // 如果找到了leader的committedLog中有zxid等于follower,observer的zxid
                // 只需要DIFF
                if (packetZxid == peerLastZxid) {
                    LOG.info("Sending DIFF zxid=0x" +
                             Long.toHexString(lastCommitedZxid) +
                             " for peer sid: " + getSid());
                    // 计入队列
                    queueOpPacket(Leader.DIFF, lastCommitedZxid);
                    needOpPacket = false;
                    continue;
                }

                // 如果是最新的zxid,0x00000000,需要DIFF
                if (isPeerNewEpochZxid) {
                   // Send diff and fall through if zxid is of a new-epoch
                   LOG.info("Sending DIFF zxid=0x" +
                            Long.toHexString(lastCommitedZxid) +
                            " for peer sid: " + getSid());
                   // 计入队列
                   queueOpPacket(Leader.DIFF, lastCommitedZxid);
                   needOpPacket = false;
                } else if (packetZxid > peerLastZxid  ) {
                    // 如果committedLog的zxid大于follower,observer的zxid
                    // Peer have some proposals that the leader hasn't seen yet
                    // it may used to be a leader
                    // 且committedLog的epoch与follower,observer的epoch不同，即不是同一代
                    // 不能进行TRUNC,返回最新的packetZxid
                    if (ZxidUtils.getEpochFromZxid(packetZxid) !=
                            ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                        // We cannot send TRUNC that cross epoch boundary.
                        // The learner will crash if it is asked to do so.
                        // We will send snapshot this those cases.
                        LOG.warn("Cannot send TRUNC to peer sid: " + getSid() +
                                 " peer zxid is from different epoch" );
                        return queuedZxid;
                    }

                    // 如果代数相同，则按committedLog最接近follower,observer的zxid进行TRUNC
                    LOG.info("Sending TRUNC zxid=0x" +
                            Long.toHexString(prevProposalZxid) +
                            " for peer sid: " + getSid());
                    queueOpPacket(Leader.TRUNC, prevProposalZxid);
                    needOpPacket = false;
                }
            }

            // 如果committedLog的zxid比上次的zxid还小，则进行下次
            if (packetZxid <= queuedZxid) {
                // We can get here, if we don't have op packet to queue
                // or there is a duplicate txn in a given iterator
                continue;
            }

            // Since this is already a committed proposal, we need to follow
            // it by a commit packet
            // 如果已经DIFF,TRUNC,且比queuedZxid要大，则将committedLog的数据计入队列
            // 将leader的commit的zxid计入队列
            queuePacket(propose.packet);
            queueOpPacket(Leader.COMMIT, packetZxid);
            // 更新遍历committedLog最新的zxid
            queuedZxid = packetZxid;

        }

        // 如果在committedLog中没有找到可比较的zxid,比如committedLog的zxid比follower,observer的zxid还要小
        // 那么只需要DIFF，即将leader的最新的zxid发给follower,observer
        if (needOpPacket && isPeerNewEpochZxid) {
            // We will send DIFF for this kind of zxid in any case. This if-block
            // is the catch when our history older than learner and there is
            // no new txn since then. So we need an empty diff
            LOG.info("Sending DIFF zxid=0x" +
                     Long.toHexString(lastCommitedZxid) +
                     " for peer sid: " + getSid());
            queueOpPacket(Leader.DIFF, lastCommitedZxid);
            needOpPacket = false;
        }

        return queuedZxid;
    }    
    
    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        leader.removeLearnerHandler(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the leader to the peers
     */
    public void ping() {
        // If learner hasn't sync properly yet, don't send ping packet
        // otherwise, the learner will crash
        if (!sendingThreadStarted) {
            return;
        }
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            synchronized(leader) {
                id = leader.lastProposed;
            }
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * Queue leader packet of a given type
     * @param type
     * @param zxid
     */
    private void queueOpPacket(int type, long zxid) {
        QuorumPacket packet = new QuorumPacket(type, zxid, null, null);
        queuePacket(packet);
    }
    
    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
    }

    public boolean synced() {
        return isAlive()
        && leader.self.tick.get() <= tickOfNextAckDeadline;
    }
    
    /**
     * For testing, return packet queue
     * @return
     */
    public Queue<QuorumPacket> getQueuedPackets() {
        return queuedPackets;
    }

    /**
     * For testing, we need to reset this value
     */
    public void setFirstPacket(boolean value) {
        needOpPacket = value;
    }
}
