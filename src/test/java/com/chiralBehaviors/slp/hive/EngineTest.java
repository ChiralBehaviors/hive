/** (C) Copyright 2010 Hal Hildebrand, All Rights Reserved
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

import static org.junit.Assert.assertEquals;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;

import org.junit.Test;

public class EngineTest {
    @Test
    public void testInetSocketAddressSerialize() throws UnknownHostException {
        InetSocketAddress written = new InetSocketAddress("localhost", 666);
        ByteBuffer buffer = ByteBuffer.allocate(256);
        Engine.write(written, buffer);
        int pos = buffer.position();
        buffer.flip();
        System.out.println(Engine.prettyPrint(new InetSocketAddress(0),
                                              written, buffer.array(), pos));
        InetSocketAddress read = Engine.readSocketAddress(buffer);
        assertEquals(written, read);
    }
}
