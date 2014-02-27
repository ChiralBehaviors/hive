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

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.UUID;

/**
 * Contains information about a specified list of Endpoints and the largest
 * version of the state they have generated as known by the local endpoint.
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Hal Hildebrand</a>
 * 
 */
public class Digest implements Comparable<Digest> {

    private final UUID id;
    private final long time;

    public Digest(ByteBuffer msg) throws UnknownHostException {
        id = new UUID(msg.getLong(), msg.getLong());
        time = msg.getLong();
    }

    public Digest(ReplicatedState state) {
        this(state.getId(), state.getTime());
    }

    public Digest(UUID id, long diffTime) {
        this.id = id;
        time = diffTime;
    }

    /* (non-Javadoc)
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Digest o) {
        int uuidCompare = id.compareTo(o.id);
        if (uuidCompare != 0) {
            return uuidCompare;
        }
        return Long.valueOf(time).compareTo(Long.valueOf(o.time));
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
        Digest other = (Digest) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        if (time != other.time) {
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

    public long getTime() {
        return time;
    }

    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (id == null ? 0 : id.hashCode());
        result = prime * result + (int) (time ^ time >>> 32);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(id);
        sb.append(":");
        sb.append(time);
        return sb.toString();
    }

    public void writeTo(ByteBuffer buffer) {
        buffer.putLong(id.getMostSignificantBits());
        buffer.putLong(id.getLeastSignificantBits());
        buffer.putLong(time);
    }
}
