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

package org.apache.zookeeper.server.persistence;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.jute.InputArchive;
import org.apache.jute.OutputArchive;
import org.apache.zookeeper.server.DataTree;
import org.apache.zookeeper.server.util.SerializeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.Adler32;
import java.util.zip.CheckedInputStream;
import java.util.zip.CheckedOutputStream;

/**
 * This class implements the snapshot interface.
 * it is responsible for storing, serializing
 * and deserializing the right snapshot.
 * and provides access to the snapshots.
 */
public class FileSnap implements SnapShot {
    File snapDir;
    private volatile boolean close = false;
    private static final int VERSION=2;
    private static final long dbId=-1;
    private static final Logger LOG = LoggerFactory.getLogger(FileSnap.class);
    public final static int SNAP_MAGIC
        = ByteBuffer.wrap("ZKSN".getBytes()).getInt();
    public FileSnap(File snapDir) {
        this.snapDir = snapDir;
    }

    /**
     * deserialize a data tree from the most recent snapshot
     * @return the zxid of the snapshot
     */ 
    public long deserialize(DataTree dt, Map<Long, Integer> sessions)
            throws IOException {
        // we run through 100 snapshots (not all of them)
        // if we cannot get it running within 100 snapshots
        // we should  give up
        // 获取倒序的snapshot file
        // 这里读取100个？经验值吧
        List<File> snapList = findNValidSnapshots(100);
        if (snapList.size() == 0) {
            return -1L;
        }
        File snap = null;
        boolean foundValid = false;
        for (int i = 0; i < snapList.size(); i++) {
            snap = snapList.get(i);
            InputStream snapIS = null;
            CheckedInputStream crcIn = null;
            try {
                LOG.info("Reading snapshot " + snap);
                snapIS = new BufferedInputStream(new FileInputStream(snap));
                crcIn = new CheckedInputStream(snapIS, new Adler32());
                InputArchive ia = BinaryInputArchive.getArchive(crcIn);
                deserialize(dt,sessions, ia);
                long checkSum = crcIn.getChecksum().getValue();
                long val = ia.readLong("val");
                if (val != checkSum) {
                    throw new IOException("CRC corruption in snapshot :  " + snap);
                }
                // 表示读取到有效的快照文件
                // 读取就跳出
                foundValid = true;
                break;
            } catch(IOException e) {
                LOG.warn("problem reading snap file " + snap, e);
            } finally {
                if (snapIS != null) 
                    snapIS.close();
                if (crcIn != null) 
                    crcIn.close();
            } 
        }
        if (!foundValid) {
            throw new IOException("Not able to find valid snapshots in " + snapDir);
        }
        // 快照文件的格式snap.zxid
        // 获取最新的快照文件的zxid
        dt.lastProcessedZxid = Util.getZxidFromName(snap.getName(), "snapshot");
        return dt.lastProcessedZxid;
    }

    /**
     * deserialize the datatree from an inputarchive
     * @param dt the datatree to be serialized into
     * @param sessions the sessions to be filled up
     * @param ia the input archive to restore from
     * @throws IOException
     */
    public void deserialize(DataTree dt, Map<Long, Integer> sessions,
            InputArchive ia) throws IOException {
        FileHeader header = new FileHeader();
        header.deserialize(ia, "fileheader");
        if (header.getMagic() != SNAP_MAGIC) {
            throw new IOException("mismatching magic headers "
                    + header.getMagic() + 
                    " !=  " + FileSnap.SNAP_MAGIC);
        }
        SerializeUtils.deserializeSnapshot(dt,ia,sessions);
    }

    /**
     * find the most recent snapshot in the database.
     * @return the file containing the most recent snapshot
     */
    public File findMostRecentSnapshot() throws IOException {
        List<File> files = findNValidSnapshots(1);
        if (files.size() == 0) {
            return null;
        }
        return files.get(0);
    }
    
    /**
     * find the last (maybe) valid n snapshots. this does some 
     * minor checks on the validity of the snapshots. It just
     * checks for / at the end of the snapshot. This does
     * not mean that the snapshot is truly valid but is
     * valid with a high probability. also, the most recent 
     * will be first on the list. 
     * @param n the number of most recent snapshots
     * @return the last n snapshots (the number might be
     * less than n in case enough snapshots are not available).
     * @throws IOException
     */
    // 获取N个有效的snapshot file
    private List<File> findNValidSnapshots(int n) throws IOException {
        // 快照文件名是prefix.zxid格式，此处的prefix为snapshot
        // 降序排列文件，即zxid从大到小排列
        List<File> files = Util.sortDataDir(snapDir.listFiles(),"snapshot", false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f : files) {
            // we should catch the exceptions
            // from the valid snapshot and continue
            // until we find a valid one
            try {
                // valid的snapshot文件最后写出了path '/'，如果不是说明快照文件生成不完整，比如断电，比如节点挂掉
                if (Util.isValidSnapshot(f)) {
                    list.add(f);
                    count++;
                    if (count == n) {
                        break;
                    }
                }
            } catch (IOException e) {
                LOG.info("invalid snapshot " + f, e);
            }
        }
        return list;
    }

    /**
     * find the last n snapshots. this does not have
     * any checks if the snapshot might be valid or not
     * @param n the number of most recent snapshots
     * @return the last n snapshots
     * @throws IOException
     */
    // 读取最近的N个snapshot file
    public List<File> findNRecentSnapshots(int n) throws IOException {
        // 升序排序snapshot file，按zxid
        // 快照文件的格式是snapshot.zxid
        List<File> files = Util.sortDataDir(snapDir.listFiles(), "snapshot", false);
        int count = 0;
        List<File> list = new ArrayList<File>();
        for (File f: files) {
            // 如果达到N个文件则结束
            if (count == n)
                break;
            if (Util.getZxidFromName(f.getName(), "snapshot") != -1) {
                count++;
                list.add(f);
            }
        }
        return list;
    }

    /**
     * serialize the datatree and sessions
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param oa the output archive to serialize into
     * @param header the header of this snapshot
     * @throws IOException
     */
    protected void serialize(DataTree dt,Map<Long, Integer> sessions,
            OutputArchive oa, FileHeader header) throws IOException {
        // this is really a programmatic error and not something that can
        // happen at runtime
        if(header==null)
            throw new IllegalStateException(
                    "Snapshot's not open for writing: uninitialized header");
        // 将FileHeader序列化，写入文件头
        header.serialize(oa, "fileheader");
        SerializeUtils.serializeSnapshot(dt,oa,sessions);
    }

    /**
     * serialize the datatree and session into the file snapshot
     * @param dt the datatree to be serialized
     * @param sessions the sessions to be serialized
     * @param snapShot the file to store snapshot into
     */
    // 这个sessions是zkDb的sessionsWithTimeouts，也就是当前存活的session
    public synchronized void serialize(DataTree dt, Map<Long, Integer> sessions, File snapShot)
            throws IOException {
        if (!close) {
            OutputStream sessOS = new BufferedOutputStream(new FileOutputStream(snapShot));
            CheckedOutputStream crcOut = new CheckedOutputStream(sessOS, new Adler32());
            //CheckedOutputStream cout = new CheckedOutputStream()
            OutputArchive oa = BinaryOutputArchive.getArchive(crcOut);
            // 写入文件头
            FileHeader header = new FileHeader(SNAP_MAGIC, VERSION, dbId);
            serialize(dt,sessions,oa, header);
            long val = crcOut.getChecksum().getValue();
            // 写入校验和
            oa.writeLong(val, "val");
            oa.writeString("/", "path");
            sessOS.flush();
            crcOut.close();
            sessOS.close();
        }
    }

    /**
     * synchronized close just so that if serialize is in place
     * the close operation will block and will wait till serialize
     * is done and will set the close flag
     */
    @Override
    public synchronized void close() throws IOException {
        close = true;
    }

 }
