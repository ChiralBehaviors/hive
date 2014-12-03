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
package com.chiralBehaviors.slp.hive;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hellblazer.utils.fd.FailureDetector;

/**
 * The Endpoint keeps track of the replicated state timestamps and the failure
 * detector for remote clients
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */

public class Endpoint {

    protected static Logger       logger    = LoggerFactory.getLogger(Endpoint.class);

    private final FailureDetector fd;
    private final Map<UUID, Long> states    = new HashMap<>();
    private final AtomicInteger   suspected = new AtomicInteger(0);

    private final ReentrantLock   synch     = new ReentrantLock();

    public Endpoint(FailureDetector fd) {
        this.fd = fd;
    }

    public Endpoint(FailureDetector fd, ReplicatedState replicatedState) {
        this(fd);
        Digest digest = replicatedState.getDigest();
        states.put(digest.getId(), digest.getTime());
    }

    public List<UUID> getUpdates(Digest[] digests) {
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            List<UUID> updates = new ArrayList<>(states.size() + 1);
            for (Digest digest : digests) {
                if (fd != null && digest.getId().equals(Common.HEARTBEAT)) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(String.format("received heartbeat"));
                    }
                    fd.record(digest.getTime(), 0);
                } else {
                    if (!states.containsKey(digest.getId())
                        || digest.getTime() > states.get(digest.getId())) {
                        updates.add(digest.getId());
                    }
                }
            }
            return updates;
        } finally {
            myLock.unlock();
        }
    }

    public Long remove(UUID state) {
        return states.remove(state);
    }

    /**
     * Answer true if the suspicion level of the failure detector is greater
     * than the conviction threshold.
     * 
     * @param now
     *            - the time at which to base the measurement
     * @param cleanUp
     *            - the number of cycles of #fail required before conviction
     * @return true if the suspicion level of the failure detector is greater
     *         than the conviction threshold
     */
    public boolean shouldConvict(long now, int cleanUp) {
        if (fd.shouldConvict(now)) {
            return suspected.incrementAndGet() >= cleanUp;
        }
        suspected.set(0);
        return false;
    }

    public void update(ReplicatedState state, EngineListener listener) {
        assert state != null : "updated state cannot be null";
        final ReentrantLock myLock = synch;
        myLock.lock();
        try {
            if (fd != null && state.getId().equals(Common.HEARTBEAT)) {
                if (logger.isTraceEnabled()) {
                    logger.trace(String.format("received heartbeat"));
                }
                fd.record(state.getTime(), 0);
            } else {
                Long time = states.get(state.getId());
                if (time == null) {
                    states.put(state.getId(), state.getTime());
                    listener.register(state.getId(), state.getState());
                } else {
                    if (time <= state.getTime()) {
                        states.put(state.getId(), state.getTime());
                        listener.update(state.getId(), state.getState());
                    }
                }
            }
        } finally {
            myLock.unlock();
        }
    }
}
