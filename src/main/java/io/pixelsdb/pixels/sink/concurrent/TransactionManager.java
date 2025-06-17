/*
 * Copyright 2025 PixelsDB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.pixelsdb.pixels.sink.concurrent;

import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;

import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * This class if for
 *
 * @author AntiO2
 */
public class TransactionManager {
    private final static TransactionManager instance = new TransactionManager();
    private final TransService transService;
    private final Queue<TransContext> transContextQueue;
    private final Object batchLock = new Object();

    TransactionManager() {
        this.transService = TransService.Instance();
        this.transContextQueue = new ConcurrentLinkedDeque<>();
    }

    public static TransactionManager Instance() {
        return instance;
    }

    private void requestTransactions() {
        try {
            List<TransContext> newContexts = transService.beginTransBatch(100, false);
            transContextQueue.addAll(newContexts);
        } catch (TransException e) {
            throw new RuntimeException("Batch request failed", e);
        }
    }

    public TransContext getTransContext() {
        TransContext ctx = transContextQueue.poll();
        if (ctx != null) {
            return ctx;
        }
        synchronized (batchLock) {
            ctx = transContextQueue.poll();
            if (ctx == null) {
                requestTransactions();
                ctx = transContextQueue.poll();
                if (ctx == null) {
                    throw new IllegalStateException("No contexts available");
                }
            }
            return ctx;
        }
    }
}
