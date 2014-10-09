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

import java.net.InetSocketAddress;
import java.util.UUID;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public interface Engine {

    void deregister(UUID id);

    InetSocketAddress getLocalAddress();

    int getMaxStateSize();

    UUID register(byte[] replicatedState);

    void setListener(EngineListener listener);

    void start();

    void stop();

    void update(UUID id, byte[] replicatedState);

}