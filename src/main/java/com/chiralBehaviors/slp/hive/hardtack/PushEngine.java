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
import static com.chiralBehaviors.slp.hive.Messages.DIGESTS;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC;
import static com.chiralBehaviors.slp.hive.Messages.MAGIC_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MAX_SEG_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.MESSAGE_HEADER_BYTE_SIZE;
import static com.chiralBehaviors.slp.hive.Messages.STATE_REQUEST;
import static com.chiralBehaviors.slp.hive.Messages.UPDATE;
import static java.lang.String.format;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Mac;
import javax.crypto.ShortBufferException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.chiralBehaviors.slp.hive.Common;
import com.chiralBehaviors.slp.hive.Engine;
import com.chiralBehaviors.slp.hive.EngineListener;
import com.chiralBehaviors.slp.hive.ReplicatedState;
import com.fasterxml.uuid.NoArgGenerator;
import com.hellblazer.utils.ByteBufferPool;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public class PushEngine implements Engine {
    private static final Logger                        log        = LoggerFactory.getLogger(PushEngine.class);

    private final DatagramSocket                       p2pSocket;
    private final AtomicBoolean                        running    = new AtomicBoolean();
    private final Mac                                  hmac;
    private final NoArgGenerator                       idGenerator;
    private final InetSocketAddress                    localAddress;
    private final ConcurrentMap<UUID, ReplicatedState> localState = new ConcurrentHashMap<>();
    private final ByteBufferPool                       bufferPool = new ByteBufferPool(
                                                                                       "Engine Comms",
                                                                                       100);
    private final int                                  heartbeatPeriod;
    private ScheduledFuture<?>                         heartbeatTask;
    private final TimeUnit                             heartbeatUnit;
    private final ScheduledExecutorService             executor;
    private final List<InetSocketAddress>              aggregators;

    public PushEngine(DatagramSocket p2pSocket, Mac mac,
                      NoArgGenerator idGenerator,
                      List<InetSocketAddress> aggregators, int heartbeatPeriod,
                      TimeUnit heartbeatUnit, ScheduledExecutorService executor) {
        this.heartbeatPeriod = heartbeatPeriod;
        this.heartbeatUnit = heartbeatUnit;
        this.aggregators = aggregators;
        this.idGenerator = idGenerator;
        hmac = mac;
        this.p2pSocket = p2pSocket;
        localAddress = (InetSocketAddress) p2pSocket.getLocalSocketAddress();
        this.executor = executor;
    }

    @Override
    public void deregister(UUID id) {
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        synchronized (localState) {
            localState.remove(id);
        }
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s abandoning replicated state",
                                    getLocalAddress()));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MESSAGE_HEADER_BYTE_SIZE);
        try {
            buffer.putLong(id.getMostSignificantBits());
            buffer.putLong(id.getLeastSignificantBits());
            send(DEREGISTER, buffer, aggregators);
        } finally {
            bufferPool.free(buffer);
        }
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
        if (replicatedState == null) {
            throw new NullPointerException("replicated state must not be null");
        }
        if (replicatedState.length > getMaxStateSize()) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             getMaxStateSize()));
        }
        UUID id = idGenerator.generate();
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    replicatedState);
        putLocalState(id, state);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s registering replicated state: %s",
                                    getLocalAddress(), id));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MESSAGE_HEADER_BYTE_SIZE);
        try {
            state.writeTo(buffer);
            send(UPDATE, buffer, aggregators);
        } finally {
            bufferPool.free(buffer);
        }
        return id;
    }

    /**
     * @param id
     * @param state
     * @return
     */
    ReplicatedState putLocalState(UUID id, ReplicatedState state) {
        return localState.put(id, state);
    }

    @Override
    public void setListener(EngineListener listener) {
    }

    @Override
    public void start() {
        if (running.compareAndSet(false, true)) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Push engine communications started on %s",
                                       localAddress));
            }
            heartbeatTask = executor.scheduleAtFixedRate(heartbeatTask(), 0,
                                                         heartbeatPeriod,
                                                         heartbeatUnit);
        }
    }

    @Override
    public void stop() {
        if (running.compareAndSet(true, false)) {
            if (log.isInfoEnabled()) {
                log.info(String.format("Terminating push UDP Communications on %s",
                                       localAddress));
            }
            heartbeatTask.cancel(true);
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
        if (id == null) {
            throw new NullPointerException(
                                           "replicated state id must not be null");
        }
        if (replicatedState == null) {
            throw new NullPointerException("replicated state must not be null");
        }
        if (replicatedState.length > getMaxStateSize()) {
            throw new IllegalArgumentException(
                                               String.format("State size %s must not be > %s",
                                                             replicatedState.length,
                                                             getMaxStateSize()));
        }
        ReplicatedState state = new ReplicatedState(id,
                                                    System.currentTimeMillis(),
                                                    replicatedState);
        putLocalState(id, state);
        if (log.isDebugEnabled()) {
            log.debug(String.format("Member: %s updating replicated state",
                                    getLocalAddress()));
        }
        ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.position(MESSAGE_HEADER_BYTE_SIZE);
        try {
            state.writeTo(buffer);
            send(UPDATE, buffer, aggregators);
        } finally {
            bufferPool.free(buffer);
        }
    }

    private synchronized void addMac(byte[] data, int offset, int length)
                                                                         throws ShortBufferException {
        hmac.reset();
        hmac.update(data, offset, length);
        hmac.doFinal(data, offset + length);
    }

    Runnable heartbeatTask() {
        return new Runnable() {
            @Override
            public void run() {
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Member: %s updating replicated state",
                                            getLocalAddress()));
                }
                replicate(new ReplicatedState(Common.HEARTBEAT,
                                              System.currentTimeMillis(),
                                              new byte[0]));
                for (ReplicatedState state : localState.values()) {
                    replicate(state);
                }
            }

            /**
             * @param state
             */
            void replicate(ReplicatedState state) {
                ByteBuffer buffer = bufferPool.allocate(MAX_SEG_SIZE);
                buffer.order(ByteOrder.BIG_ENDIAN);
                buffer.position(MESSAGE_HEADER_BYTE_SIZE);
                try {
                    state.writeTo(buffer);
                    send(UPDATE, buffer, aggregators);
                } finally {
                    bufferPool.free(buffer);
                }
            }
        };
    }

    private void send(byte msgType, ByteBuffer buffer,
                      List<InetSocketAddress> targets) {
        if (p2pSocket.isClosed()) {
            log.trace(String.format("Sending on a closed socket"));
            return;
        }
        int msgLength = buffer.position();
        int totalLength = msgLength + hmac.getMacLength();
        buffer.putInt(0, MAGIC);
        buffer.put(MAGIC_BYTE_SIZE, msgType);
        byte[] bytes = buffer.array();
        try {
            addMac(bytes, 0, msgLength);
        } catch (ShortBufferException e) {
            log.error("Invalid message (%s) %s",
                      type(msgType),
                      Common.prettyPrint(getLocalAddress(), targets,
                                         buffer.array(), msgLength));
            return;
        } catch (SecurityException e) {
            log.error("No key provided for HMAC");
            return;
        }
        for (InetSocketAddress target : targets) {
            try {
                DatagramPacket packet = new DatagramPacket(bytes, totalLength,
                                                           target);
                if (log.isTraceEnabled()) {
                    log.trace(String.format("sending %s packet mac start: %s %s",
                                            type(msgType),
                                            msgLength,
                                            Common.prettyPrint(getLocalAddress(),
                                                               target,
                                                               buffer.array(),
                                                               totalLength)));
                }
                p2pSocket.send(packet);
            } catch (SocketException e) {
                if (!"Socket is closed".equals(e.getMessage())
                    && !"Bad file descriptor".equals(e.getMessage())) {
                    if (log.isWarnEnabled()) {
                        log.warn("Error sending packet", e);
                    }
                }
            } catch (IOException e) {
                if (log.isWarnEnabled()) {
                    log.warn(String.format("Error sending packet to: %s",
                                           targets), e);
                }
            }
        }
    }

    /**
     * @param msgType
     * @return
     */
    private String type(byte msgType) {
        switch (msgType) {
            case DIGESTS: {
                return "digests";
            }
            case UPDATE: {
                return "update";
            }
            case DEREGISTER: {
                return "deregister";
            }
            case STATE_REQUEST: {
                return "state request";
            }
            default: {
                if (log.isInfoEnabled()) {
                    log.info(format("invalid message type: %s", msgType));
                }
                throw new IllegalArgumentException(
                                                   String.format("Invalid message type %s",
                                                                 msgType));
            }
        }
    }

}