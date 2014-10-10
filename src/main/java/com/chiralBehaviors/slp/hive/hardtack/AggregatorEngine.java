/** (C) Copyright 2014 Chiral Behaviors, All Rights Reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.chiralBehaviors.slp.hive.hardtack;

import static com.chiralBehaviors.slp.hive.Messages.DEREGISTER;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC;
import static com.chiralBehaviors.slp.hive.Messages.MAX_SEG_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MESSAGE_HEADER_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.UPDATE;
import static java.lang.String.format;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Mac;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralBehaviors.slp.hive.Common;
import com.chiralBehaviors.slp.hive.Digest;
import com.chiralBehaviors.slp.hive.Endpoint;
import com.chiralBehaviors.slp.hive.Engine;
import com.chiralBehaviors.slp.hive.EngineListener;
import com.chiralBehaviors.slp.hive.ReplicatedState;
import com.hellblazer.utils.ByteBufferPool;
import com.hellblazer.utils.fd.FailureDetectorFactory;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public class AggregatorEngine implements Engine {
    private static final Logger                              log        = LoggerFactory.getLogger(AggregatorEngine.class);

    private final ByteBufferPool                             bufferPool = new ByteBufferPool(
                                                                                             "Engine Comms",
                                                                                             100);
    private final ScheduledExecutorService                   executor;
    private final FailureDetectorFactory                     fdFactory;
    private final Mac                                        hmac;
    private EngineListener                                   listener;
    private final InetSocketAddress                          localAddress;
    private final ConcurrentMap<InetSocketAddress, Endpoint> members    = new ConcurrentHashMap<>();
    private final DatagramSocket                             p2pSocket;
    private final AtomicBoolean                              running    = new AtomicBoolean();

    public AggregatorEngine(DatagramSocket p2pSocket, Mac mac,
                            ScheduledExecutorService executor,
                            FailureDetectorFactory fdFactory) {
        this.executor = executor;
        this.fdFactory = fdFactory;
        hmac = mac;
        this.p2pSocket = p2pSocket;
        localAddress = (InetSocketAddress) p2pSocket.getLocalSocketAddress();
    }

    @Override
    public void deregister(UUID id) {
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return localAddress;
    }

    @Override
    public int getMaxStateSize() {
        return MAX_SEG_SIZE - hmac.getMacLength() - MESSAGE_HEADER_BYTE_SIZE;
    }

    @Override
    public UUID register(byte[] replicatedState) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setListener(EngineListener listener) {
        this.listener = listener;
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            Executors.newSingleThreadExecutor().execute(p2pServiceTask());
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Terminating UDP Communications on %s",
                                       localAddress));
            }
            p2pSocket.close();
            log.info(bufferPool.toString());
        }
    }

    @Override
    public String toString() {
        return String.format("BroadcastComms[%s]", getLocalAddress());
    }

    @Override
    public void update(UUID id, byte[] replicatedState) {
    }

    private synchronized boolean checkMac(byte[] data, int start, int length) {
        hmac.reset();
        hmac.update(data, start, length);
        byte[] checkMAC = hmac.doFinal();
        int len = checkMAC.length;
        assert len == hmac.getMacLength();

        for (int i = 0; i < len; i++) {
            if (checkMAC[i] != data[start + length + i]) {
                return false;
            }
        }
        return true;
    }

    private void service(final DatagramSocket socket, final String tag)
                                                                       throws IOException {
        final ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        final DatagramPacket packet = new DatagramPacket(buffer.array(),
                                                         buffer.array().length);
        if (log.isTraceEnabled()) {
            log.trace(String.format("listening for %s packet on %s", tag,
                                    socket.getLocalSocketAddress()));
        }
        socket.receive(packet);
        buffer.limit(packet.getLength());
        if (log.isTraceEnabled()) {
            log.trace(String.format("Received %s packet %s",
                                    tag,
                                    Common.prettyPrint(packet.getSocketAddress(),
                                                       getLocalAddress(),
                                                       buffer.array(),
                                                       packet.getLength())));
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int magic = buffer.getInt();
                if (MAGIC == magic) {
                    try {
                        if (!checkMac(buffer.array(), 0, packet.getLength()
                                                         - hmac.getMacLength())) {
                            if (log.isWarnEnabled()) {
                                log.warn(format("Error processing inbound %s message on: %s, HMAC does not check",
                                                tag,
                                                socket.getLocalSocketAddress()));
                            }
                            return;
                        }
                    } catch (SecurityException e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing %s inbound message on: %s, HMAC does not check",
                                            tag, socket.getLocalSocketAddress()),
                                     e);
                        }
                        return;
                    }
                    buffer.limit(packet.getLength() - hmac.getMacLength());
                    try {
                        processInbound((InetSocketAddress) packet.getSocketAddress(),
                                       buffer);
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn(format("Error processing %s inbound message on: %s",
                                            tag, socket.getLocalSocketAddress()),
                                     e);
                        }
                    }
                } else {
                    if (log.isWarnEnabled()) {
                        log.warn(format("%s msg with invalid MAGIC header [%s] discarded %s",
                                        tag,
                                        magic,
                                        Common.prettyPrint(packet.getSocketAddress(),
                                                           getLocalAddress(),
                                                           buffer.array(),
                                                           packet.getLength())));
                    }
                }
                bufferPool.free(buffer);
            }
        });
    }

    protected UUID[] extractIds(SocketAddress sender, ByteBuffer msg) {
        int count = msg.get();
        final UUID[] uuids = new UUID[count];
        for (int i = 0; i < count; i++) {
            UUID id;
            try {
                id = new UUID(msg.getLong(), msg.getLong());
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Cannot deserialize uuid. Ignoring the uuid: %s\n%s",
                                           i,
                                           Common.prettyPrint(sender,
                                                              getLocalAddress(),
                                                              msg.array(),
                                                              msg.limit())), e);
                }
                continue;
            }
            uuids[i] = id;
        }
        return uuids;
    }

    protected void handleUpdate(InetSocketAddress sender, ByteBuffer buffer) {
        if (sender.equals(localAddress)) {
            if (log.isTraceEnabled()) {
                log.trace(format("Ignoring update from self %s", sender));
            }
            return;
        }
        final ReplicatedState state;
        try {
            state = new ReplicatedState(buffer);
        } catch (Throwable e) {
            if (log.isWarnEnabled()) {
                log.warn("Cannot deserialize state. Ignoring the state.", e);
            }
            return;
        }
        Endpoint endpoint = new Endpoint(fdFactory.create());
        Endpoint prev = members.putIfAbsent(sender, endpoint);
        if (prev != null) {
            endpoint = prev;
        }
        endpoint.update(state, listener);
        if (log.isTraceEnabled()) {
            log.trace(format("Update state %s from %s is : %s", state.getId(),
                             sender, state));
        }
    }

    protected Runnable serviceTask(final DatagramSocket socket, final String tag) {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isInfoEnabled()) {
                    log.info(String.format("UDP %s communications started on %s",
                                           tag, socket.getLocalSocketAddress()));
                }
                while (running.get()) {
                    try {
                        service(socket, tag);
                    } catch (SocketException e) {
                        if ("Socket closed".equals(e.getMessage())) {
                            if (log.isTraceEnabled()) {
                                log.trace("Socket closed, shutting down");
                                stop();
                                return;
                            }
                        }
                    } catch (Throwable e) {
                        if (log.isWarnEnabled()) {
                            log.warn("Exception processing inbound message", e);
                        }
                    }
                }
            }
        };
    }

    Digest[] extractDigests(SocketAddress sender, ByteBuffer msg) {
        int count = msg.get();
        final Digest[] digests = new Digest[count];
        for (int i = 0; i < count; i++) {
            Digest digest;
            try {
                digest = new Digest(msg);
            } catch (Throwable e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Cannot deserialize digest. Ignoring the digest: %s\n%s",
                                           i,
                                           Common.prettyPrint(sender,
                                                              getLocalAddress(),
                                                              msg.array(),
                                                              msg.limit())), e);
                }
                continue;
            }
            digests[i] = digest;
        }
        return digests;
    }

    void handleDeregister(InetSocketAddress sender, ByteBuffer buffer) {
        if (sender.equals(localAddress)) {
            if (log.isTraceEnabled()) {
                log.trace(format("Ignoring deregister from self %s", sender));
            }
            return;
        }
        UUID stateId = new UUID(buffer.getLong(), buffer.getLong());
        Endpoint endpoint = members.get(sender);
        if (endpoint == null) {
            log.trace(String.format("Remove %s from unknown member %s",
                                    stateId, sender));
            return;
        }
        if (log.isTraceEnabled()) {
            log.trace(format("Deregistering %s from %s", stateId, sender));
        }
        endpoint.remove(stateId);
        listener.deregister(stateId);
    }

    /**
     * @return
     */
    Runnable p2pServiceTask() {
        return serviceTask(p2pSocket, "p2p");
    }

    void processInbound(InetSocketAddress sender, ByteBuffer buffer)
                                                                    throws UnknownHostException {
        byte msgType = buffer.get();
        switch (msgType) {
            case UPDATE: {
                handleUpdate(sender, buffer);
                break;
            }
            case DEREGISTER: {
                handleDeregister(sender, buffer);
                break;
            }
            default: {
                if (log.isInfoEnabled()) {
                    log.info(format("invalid message type: %s from: %s",
                                    msgType, this));
                }
            }
        }
    }

}