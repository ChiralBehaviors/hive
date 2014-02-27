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

/**
 * The communications constants used by the broadcast protocol
 * 
 * @author <a href="mailto:hal.hildebrand@gmail.com">Chiral Behaviors</a>
 * 
 */
public interface Messages {
    int  BYTE_SIZE                = 1;
    int  INT_BYTE_SIZE            = 4;
    int  LONG_BYTE_SIZE           = INT_BYTE_SIZE + INT_BYTE_SIZE;
    int  MAGIC                    = 0xCAFEBABE;
    int  MAGIC_BYTE_SIZE          = INT_BYTE_SIZE;
    /**
     * MAX_SEG_SIZE is a default maximum packet size. This may be small, but any
     * network will be capable of handling this size so the packet transfer
     * semantics are atomic (no fragmentation in the network).
     */
    int  MAX_SEG_SIZE             = 1500;
    int  MESSAGE_HEADER_BYTE_SIZE = MAGIC_BYTE_SIZE + BYTE_SIZE;
    int  UUID_BYTE_SIZE           = LONG_BYTE_SIZE + LONG_BYTE_SIZE;
    int  DIGEST_BYTE_SIZE         = UUID_BYTE_SIZE + LONG_BYTE_SIZE;

    /** Messages **/
    byte DIGESTS                  = 0;
    byte UPDATE                   = 1;
    byte REMOVE                   = 2;
    byte STATE_REQUEST            = 3;
}