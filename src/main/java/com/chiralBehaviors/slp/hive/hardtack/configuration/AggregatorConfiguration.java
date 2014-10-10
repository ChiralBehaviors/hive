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

package com.chiralBehaviors.slp.hive.hardtack.configuration;

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.concurrent.Executors;

import com.chiralBehaviors.slp.hive.Engine;
import com.chiralBehaviors.slp.hive.hardtack.AggregatorEngine;
import com.hellblazer.utils.Utils;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public class AggregatorConfiguration extends HardtackConfiguration {

    public static final int DEFAULT_PORT = 56999;

    public int               port         = DEFAULT_PORT;

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.configuration.EngineConfiguration#construct()
     */
    @Override
    public Engine construct() throws IOException {
        NetworkInterface intf = getNetworkInterface();
        DatagramSocket socket = new DatagramSocket(
                                                   new InetSocketAddress(
                                                                         Utils.getAddress(intf,
                                                                                          ipv4),
                                                                         port));
        return new AggregatorEngine(
                                    socket,
                                    getMac(),
                                    Executors.newSingleThreadScheduledExecutor(),
                                    getFdFactory());
    }
}
