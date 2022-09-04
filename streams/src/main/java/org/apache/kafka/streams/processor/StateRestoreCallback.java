/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.processor;

/**
 * Restoration logic for log-backed state stores upon restart,
 * it takes one record at a time from the logs to apply to the restoring state.
 */
public interface StateRestoreCallback {

    void restore(byte[] key, byte[] value);

    // some similarities to (optional) user-provided StateRestoreListener#onRestoreEnd()
    // but this is part of the restore process (restore is not complete until this has executed)
    default void finishRestore() {
    }
}
