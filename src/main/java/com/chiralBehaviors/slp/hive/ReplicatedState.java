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

import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * @author hhildebrand
 * 
 */
public class ReplicatedState {
    public static byte[] getState(ByteBuffer buffer) {
        byte[] state = new byte[buffer.remaining()];
        buffer.get(state);
        return state;
    }

    private final UUID   id;
    private final byte[] state;
    private final long   time;

    public ReplicatedState(ByteBuffer buffer) {
        this(getUUID(buffer), buffer.getLong(), getState(buffer));
    }

    /**
     * @param buffer
     * @return
     */
    private static UUID getUUID(ByteBuffer buffer) {
        return new UUID(buffer.getLong(), buffer.getLong());
    }

    public ReplicatedState(UUID id, long time, byte[] state) {
        this.id = id;
        this.state = state;
        this.time = time;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ReplicatedState other = (ReplicatedState) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

    /**
     * @return the id
     */
    public UUID getId() {
        return id;
    }

    /**
     * @return the state
     */
    public byte[] getState() {
        return state;
    }

    /**
     * @return the time
     */
    public long getTime() {
        return time;
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public boolean isDeleted() {
        return state.length == 0 && !isHeartbeat();
    }

    public boolean isEmpty() {
        return state.length == 0;
    }

    public boolean isHeartbeat() {
        return Engine.HEARTBEAT.equals(id);
    }

    public boolean isNotifiable() {
        return state.length > 0 && !isHeartbeat();
    }

    @Override
    public String toString() {
        return String.format("ReplicatedState [id=%s,size=%s]", id,
                             state.length);
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        buffer.putLong(time);
        buffer.put(state);
    }

    public Digest getDigest() {
        return new Digest(this);
    }
}
