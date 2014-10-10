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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import com.chiralBehaviors.slp.hive.Engine;
import com.chiralBehaviors.slp.hive.hardtack.PushEngine;
import com.fasterxml.uuid.Generators;
import com.hellblazer.utils.Utils;

/**
 * @author <a href="mailto:hal.hildebrand@me.com">Chiral Behaviors</a>
 *
 */
public class PushConfiguration extends HardtackConfiguration {

    public List<InetSocketAddress> aggregators = Arrays.asList(new InetSocketAddress(
                                                                                     Integer.valueOf(AggregatorConfiguration.DEFAULT_PORT)));

    /* (non-Javadoc)
     * @see com.chiralBehaviors.slp.hive.configuration.EngineConfiguration#construct()
     */
    @Override
    public Engine construct() throws IOException {
        NetworkInterface intf = getNetworkInterface();
        InetAddress address = Utils.getAddress(intf, ipv4);
        DatagramSocket p2pSocket = new DatagramSocket(
                                                      new InetSocketAddress(
                                                                            address,
                                                                            Utils.allocatePort(address)));
        int i = 0;
        for (InetSocketAddress aggregator : aggregators) {
            if (aggregator.getAddress().isAnyLocalAddress()) {
                aggregators.set(i++,
                                new InetSocketAddress(address,
                                                      aggregator.getPort()));
            }
        }
        return new PushEngine(p2pSocket, getMac(),
                              Generators.timeBasedGenerator(), aggregators,
                              heartbeatPeriod, heartbeatUnit,
                              Executors.newSingleThreadScheduledExecutor());

    }
}
