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


import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.common.X509Exception;
import org.apache.zookeeper.common.X509Exception.SSLContextException;
import org.apache.zookeeper.common.X509Util;
import org.apache.zookeeper.common.ZKConfig;
import org.apache.zookeeper.server.auth.ProviderRegistry;
import org.apache.zookeeper.server.auth.X509AuthenticationProvider;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.ChannelHandler.Sharable;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.ssl.SslHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;


public class NettyServerCnxnFactory extends ServerCnxnFactory {
    private static final Logger LOG = LoggerFactory.getLogger(NettyServerCnxnFactory.class);

    // 服务端连接器
    ServerBootstrap bootstrap;
    // 这个channel是server channel，每次连接会创建child channel
    Channel parentChannel;
    // 所有channel所属的group
    ChannelGroup allChannels = new DefaultChannelGroup("zkServerCnxns");
    HashMap<InetAddress, Set<NettyServerCnxn>> ipMap =
        new HashMap<InetAddress, Set<NettyServerCnxn>>( );
    InetSocketAddress localAddress;
    // 最大连接数
    int maxClientCnxns = 60;

    /**
     * This is an inner class since we need to extend SimpleChannelHandler, but
     * NettyServerCnxnFactory already extends ServerCnxnFactory. By making it inner
     * this class gets access to the member variables and methods.
     */
    // 设置连接控制的handler
    @Sharable
    class CnxnChannelHandler extends SimpleChannelHandler {

        @Override
        public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
            throws Exception
        {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Channel closed " + e);
            }
            allChannels.remove(ctx.getChannel());
        }

        /**
         * 连接时处理
         * @param ctx
         * @param e
         * @throws Exception
         */
        @Override
        public void channelConnected(ChannelHandlerContext ctx,
                ChannelStateEvent e) throws Exception
        {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Channel connected " + e);
            }

            // 当connected的时候，创建连接对象
            NettyServerCnxn cnxn = new NettyServerCnxn(ctx.getChannel(),
                    zkServer, NettyServerCnxnFactory.this);
            // 添加到attachment
            ctx.setAttachment(cnxn);

            if (secure) {
                SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);
                ChannelFuture handshakeFuture = sslHandler.handshake();
                // CertificateVerifier这个listener在operationComplete的时候，会将channel添加到allChannels，并添加cnxn
                handshakeFuture.addListener(new CertificateVerifier(sslHandler, cnxn));
            } else {
                // 添加到channelGroup
                allChannels.add(ctx.getChannel());
                addCnxn(cnxn);
            }
        }

        /**
         * 连接断开处理
         * @param ctx
         * @param e
         * @throws Exception
         */
        @Override
        public void channelDisconnected(ChannelHandlerContext ctx,
                ChannelStateEvent e) throws Exception
        {
            if (LOG.isTraceEnabled()) {
                LOG.trace("Channel disconnected " + e);
            }

            // 从context获取attachment，然后关闭连接
            NettyServerCnxn cnxn = (NettyServerCnxn) ctx.getAttachment();
            if (cnxn != null) {
                if (LOG.isTraceEnabled()) {
                    LOG.trace("Channel disconnect caused close " + e);
                }
                cnxn.close();
            }
        }

        /**
         * 异常处理
         *
         * @param ctx
         * @param e
         * @throws Exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
            throws Exception
        {
            LOG.warn("Exception caught " + e, e.getCause());
            NettyServerCnxn cnxn = (NettyServerCnxn) ctx.getAttachment();
            if (cnxn != null) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Closing " + cnxn);
                }
                cnxn.close();
            }
        }

        /**
         * 接收到消息处理
         *
         * @param ctx
         * @param e
         * @throws Exception
         */
        @Override
        public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
            throws Exception
        {
            if (LOG.isTraceEnabled()) {
                LOG.trace("message received called " + e.getMessage());
            }
            try {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("New message " + e.toString()
                            + " from " + ctx.getChannel());
                }
                NettyServerCnxn cnxn = (NettyServerCnxn)ctx.getAttachment();
                synchronized(cnxn) {
                    processMessage(e, cnxn);
                }
            } catch(Exception ex) {
                LOG.error("Unexpected exception in receive", ex);
                throw ex;
            }
        }

        private void processMessage(MessageEvent e, NettyServerCnxn cnxn) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(Long.toHexString(cnxn.sessionId) + " queuedBuffer: "
                        + cnxn.queuedBuffer);
            }

            /**
             * 如果是启用receive message事件
             * @see NettyServerCnxn#enableRecv()
             */
            if (e instanceof NettyServerCnxn.ResumeMessageEvent) {
                LOG.debug("Received ResumeMessageEvent");
                if (cnxn.queuedBuffer != null) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("processing queue "
                                + Long.toHexString(cnxn.sessionId)
                                + " queuedBuffer 0x"
                                + ChannelBuffers.hexDump(cnxn.queuedBuffer));
                    }
                    cnxn.receiveMessage(cnxn.queuedBuffer);
                    // 如果queuedBuffer已经被全部读取
                    if (!cnxn.queuedBuffer.readable()) {
                        LOG.debug("Processed queue - no bytes remaining");
                        cnxn.queuedBuffer = null;
                    } else {
                        LOG.debug("Processed queue - bytes remaining");
                    }
                } else {
                    LOG.debug("queue empty");
                }
                // 设置channel为可读
                cnxn.channel.setReadable(true);
            } else {
                // 获取数据
                ChannelBuffer buf = (ChannelBuffer)e.getMessage();
                if (LOG.isTraceEnabled()) {
                    LOG.trace(Long.toHexString(cnxn.sessionId)
                            + " buf 0x"
                            + ChannelBuffers.hexDump(buf));
                }

                // throttled为true，表示禁止读取message
                if (cnxn.throttled) {
                    LOG.debug("Received message while throttled");
                    // we are throttled, so we need to queue
                    if (cnxn.queuedBuffer == null) {
                        LOG.debug("allocating queue");
                        cnxn.queuedBuffer = ChannelBuffers.dynamicBuffer(buf.readableBytes());
                    }

                    // 将数据写出到cnxn的queuedBuffer中
                    // 这里cnxn并没有立即receiveMessage，只是将其读入到queuedBuffer
                    cnxn.queuedBuffer.writeBytes(buf);
                    if (LOG.isTraceEnabled()) {
                        LOG.trace(Long.toHexString(cnxn.sessionId)
                                + " queuedBuffer 0x"
                                + ChannelBuffers.hexDump(cnxn.queuedBuffer));
                    }
                } else {// 如果没有限制读取数据
                    LOG.debug("not throttled");
                    // 如果queuedBuffer已经存在，那么message写出到queuedBuffer中去，因为queuedBuffer已经有之前的请求数据了，不能丢弃
                    if (cnxn.queuedBuffer != null) {
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(Long.toHexString(cnxn.sessionId)
                                    + " queuedBuffer 0x"
                                    + ChannelBuffers.hexDump(cnxn.queuedBuffer));
                        }
                        cnxn.queuedBuffer.writeBytes(buf);
                        if (LOG.isTraceEnabled()) {
                            LOG.trace(Long.toHexString(cnxn.sessionId)
                                    + " queuedBuffer 0x"
                                    + ChannelBuffers.hexDump(cnxn.queuedBuffer));
                        }

                        // cnxn内部统一读取全部的message
                        cnxn.receiveMessage(cnxn.queuedBuffer);
                        if (!cnxn.queuedBuffer.readable()) {
                            LOG.debug("Processed queue - no bytes remaining");
                            // 数据读取完毕，销毁queuedBuffer
                            cnxn.queuedBuffer = null;
                        } else {
                            LOG.debug("Processed queue - bytes remaining");
                        }
                    } else {
                        // 如果queuedBuffer不存在，就直接读取
                        cnxn.receiveMessage(buf);
                        // 为什么要再读一次呢？
                        // 因为在读取的过程中，如果出现了禁止读取，即throttled=true，buf就无法读取完毕
                        // 那么将尚未读取完毕的数据，写出到queuedBuffer中去
                        if (buf.readable()) {
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Before copy " + buf);
                            }
                            cnxn.queuedBuffer = ChannelBuffers.dynamicBuffer(buf.readableBytes());
                            cnxn.queuedBuffer.writeBytes(buf);
                            if (LOG.isTraceEnabled()) {
                                LOG.trace("Copy is " + cnxn.queuedBuffer);
                                LOG.trace(Long.toHexString(cnxn.sessionId)
                                        + " queuedBuffer 0x"
                                        + ChannelBuffers.hexDump(cnxn.queuedBuffer));
                            }
                        }
                    }
                }
            }
        }

        /**
         * 当写出数据完成时触发，这里只是简单的打印了log
         *
         * @param ctx
         * @param e
         * @throws Exception
         */
        @Override
        public void writeComplete(ChannelHandlerContext ctx,
                WriteCompletionEvent e) throws Exception
        {
            if (LOG.isTraceEnabled()) {
                LOG.trace("write complete " + e);
            }
        }

        /**
         * 身份验证的listener专门处理ssl
         */
        private final class CertificateVerifier
                implements ChannelFutureListener {
            private final SslHandler sslHandler;
            private final NettyServerCnxn cnxn;

            CertificateVerifier(SslHandler sslHandler, NettyServerCnxn cnxn) {
                this.sslHandler = sslHandler;
                this.cnxn = cnxn;
            }

            /**
             * Only allow the connection to stay open if certificate passes auth
             */
            public void operationComplete(ChannelFuture future)
                    throws SSLPeerUnverifiedException {
                if (future.isSuccess()) {
                    LOG.debug("Successful handshake with session 0x{}",
                            Long.toHexString(cnxn.sessionId));
                    SSLEngine eng = sslHandler.getEngine();
                    SSLSession session = eng.getSession();
                    // 保存所有的验证信息
                    cnxn.setClientCertificateChain(session.getPeerCertificates());

                    String authProviderProp
                            = System.getProperty(ZKConfig.SSL_AUTHPROVIDER, "x509");

                    X509AuthenticationProvider authProvider =
                            (X509AuthenticationProvider)
                                    ProviderRegistry.getProvider(authProviderProp);

                    if (authProvider == null) {
                        LOG.error("Auth provider not found: {}", authProviderProp);
                        cnxn.close();
                        return;
                    }

                    if (KeeperException.Code.OK !=
                            authProvider.handleAuthentication(cnxn, null)) {
                        LOG.error("Authentication failed for session 0x{}",
                                Long.toHexString(cnxn.sessionId));
                        cnxn.close();
                        return;
                    }

                    allChannels.add(future.getChannel());
                    addCnxn(cnxn);
                } else {
                    LOG.error("Unsuccessful handshake with session 0x{}",
                            Long.toHexString(cnxn.sessionId));
                    cnxn.close();
                }
            }
        }
    }

    // 实例化channelHandler
    CnxnChannelHandler channelHandler = new CnxnChannelHandler();

    /**
     * 默认的构造器，初始化连接配置
     * 连接就是普通的tcp连接
     */
    NettyServerCnxnFactory() {
        bootstrap = new ServerBootstrap(
                new NioServerSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
        // parent channel
        // 设置服务器端口重用
        bootstrap.setOption("reuseAddress", true);
        // child channels
        bootstrap.setOption("child.tcpNoDelay", true);
        /* set socket linger to off, so that socket close does not block */
        bootstrap.setOption("child.soLinger", -1);
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
            @Override
            public ChannelPipeline getPipeline() throws Exception {
                ChannelPipeline p = Channels.pipeline();
                if (secure) {
                    initSSL(p);
                }
                p.addLast("servercnxnfactory", channelHandler);

                return p;
            }
        });
    }

    private synchronized void initSSL(ChannelPipeline p)
            throws X509Exception, KeyManagementException, NoSuchAlgorithmException {
        String authProviderProp = System.getProperty(ZKConfig.SSL_AUTHPROVIDER);
        SSLContext sslContext;
        if (authProviderProp == null) {
            sslContext = X509Util.createSSLContext();
        } else {
            sslContext = SSLContext.getInstance("TLSv1");
            X509AuthenticationProvider authProvider =
                    (X509AuthenticationProvider)ProviderRegistry.getProvider(
                            System.getProperty(ZKConfig.SSL_AUTHPROVIDER,
                                    "x509"));

            if (authProvider == null)
            {
                LOG.error("Auth provider not found: {}", authProviderProp);
                throw new SSLContextException(
                        "Could not create SSLContext with specified auth provider: " +
                        authProviderProp);
            }

            sslContext.init(new X509KeyManager[] { authProvider.getKeyManager() },
                            new X509TrustManager[] { authProvider.getTrustManager() },
                            null);
        }

        SSLEngine sslEngine = sslContext.createSSLEngine();
        sslEngine.setUseClientMode(false);
        sslEngine.setNeedClientAuth(true);

        // 将SslHandler注册到pipeline
        p.addLast("ssl", new SslHandler(sslEngine));
        LOG.info("SSL handler added for channel: {}", p.getChannel());
    }

    /**
     * 关闭所有的连接
     */
    @Override
    public void closeAll() {
        if (LOG.isDebugEnabled()) {
            LOG.debug("closeAll()");
        }
        // clear all the connections on which we are selecting
        int length = cnxns.size();
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close();
            } catch (Exception e) {
                LOG.warn("Ignoring exception closing cnxn sessionid 0x"
                         + Long.toHexString(cnxn.getSessionId()), e);
            }
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("allChannels size:" + allChannels.size() + " cnxns size:"
                    + length);
        }
    }

    /**
     * 关闭指定的连接
     * @param sessionId
     * @return
     */
    @Override
    public boolean closeSession(long sessionId) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("closeSession sessionid:0x" + sessionId);
        }
        for (ServerCnxn cnxn : cnxns) {
            if (cnxn.getSessionId() == sessionId) {
                try {
                    cnxn.close();
                } catch (Exception e) {
                    LOG.warn("exception during session close", e);
                }
                return true;
            }
        }
        return false;
    }

    /**
     * 配置连接的参数
     * @param addr              本地address
     * @param maxClientCnxns    最大连接数目
     * @param secure            是否启用ssl模式
     * @throws IOException
     */
    @Override
    public void configure(InetSocketAddress addr, int maxClientCnxns, boolean secure)
            throws IOException
    {
        configureSaslLogin();
        localAddress = addr;
        this.maxClientCnxns = maxClientCnxns;
        this.secure = secure;
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    @Override
    public int getLocalPort() {
        return localAddress.getPort();
    }

    // 是否关闭连接工厂
    boolean killed;
    @Override
    public void join() throws InterruptedException {
        synchronized(this) {
            while(!killed) {
                wait();
            }
        }
    }

    /**
     * 关闭连接工厂
     */
    @Override
    public void shutdown() {
        LOG.info("shutdown called " + localAddress);
        if (login != null) {
            login.shutdown();
        }
        // null if factory never started
        if (parentChannel != null) {
            parentChannel.close().awaitUninterruptibly();
            closeAll();
            allChannels.close().awaitUninterruptibly();
            bootstrap.releaseExternalResources();
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
        synchronized(this) {
            killed = true;
            notifyAll();
        }
    }

    /**
     * 启动连接工厂
     */
    @Override
    public void start() {
        LOG.info("binding to port " + localAddress);
        // 启动服务器
        parentChannel = bootstrap.bind(localAddress);
    }
    
    public void reconfigure(InetSocketAddress addr) {
       Channel oldChannel = parentChannel;
       try {
           LOG.info("binding to port {}", addr);
           parentChannel = bootstrap.bind(addr);
           localAddress = addr;
       } catch (Exception e) {
           LOG.error("Error while reconfiguring", e);
       } finally {
           oldChannel.close();
       }
    }
    
    @Override
    public void startup(ZooKeeperServer zks, boolean startServer)
            throws IOException, InterruptedException {
        start();
        setZooKeeperServer(zks);
        if (startServer) {
            zks.startdata();
            zks.startup();
        }
    }

    /**
     * 获取所有的连接
     * @return
     */
    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    /**
     * 获取本服务器的连接地址
     * @return
     */
    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    /**
     * 保存所有的cnxn对象，
     * 并将所有客户端的连接地址，对应的连接对象保存到ipMap中 TODO 是客户端连接地址???
     * @param cnxn
     */
    private void addCnxn(NettyServerCnxn cnxn) {
        cnxns.add(cnxn);
        synchronized (ipMap){
            InetAddress addr =
                ((InetSocketAddress)cnxn.channel.getRemoteAddress())
                    .getAddress();
            Set<NettyServerCnxn> s = ipMap.get(addr);
            if (s == null) {
                s = new HashSet<NettyServerCnxn>();
            }
            s.add(cnxn);
            ipMap.put(addr,s);
        }
    }

    /**
     * 重置所有连接的统计
     */
    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for(ServerCnxn c : cnxns){
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String,Object>> info = new HashSet<Map<String,Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }

}
