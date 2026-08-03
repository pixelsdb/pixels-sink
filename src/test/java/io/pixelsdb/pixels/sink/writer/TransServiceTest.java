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
 */
package io.pixelsdb.pixels.sink.writer;


import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@Tag("integration")
class TransServiceTest
{
    private static final Logger logger = LoggerFactory.getLogger(TransServiceTest.class);

    @Test
    public void testTransactionService()
    {
        int numTransactions = Integer.getInteger("pixels.sink.test.transactions", 10);

        TransService transService = TransService.Instance();
        try
        {
            List<TransContext> transContexts = transService.beginTransBatch(numTransactions, false);
            assertEquals(numTransactions, transContexts.size());
            TransContext prevTransContext = transContexts.get(0);
            for (int i = 1; i < numTransactions; i++)
            {
                TransContext transContext = transContexts.get(i);
                assertTrue(transContext.getTransId() > prevTransContext.getTransId());
                assertTrue(transContext.getTimestamp() > prevTransContext.getTimestamp());
                prevTransContext = transContext;
            }
        } catch (TransException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBatchRequest()
    {
        int numTransactions = Integer.getInteger("pixels.sink.test.batch.transactions", 100);

        TransService transService = TransService.Instance();
        try
        {
            List<TransContext> transContexts = transService.beginTransBatch(numTransactions, false);
            assertEquals(numTransactions, transContexts.size());
            TransContext prevTransContext = transContexts.get(0);
            for (int i = 1; i < numTransactions; i++)
            {
                TransContext transContext = transContexts.get(i);
                assertTrue(transContext.getTransId() > prevTransContext.getTransId());
                assertTrue(transContext.getTimestamp() > prevTransContext.getTimestamp());
                prevTransContext = transContext;
            }
        } catch (TransException e)
        {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAbort() throws TransException
    {
        TransService transService = TransService.Instance();
        TransContext transContext = transService.beginTrans(true);

        logger.info("ID {}, TS {}", transContext.getTransId(), transContext.getTimestamp());
        TransContext transContext1 = transService.beginTrans(false);
        TransContext transContext2 = transService.beginTrans(false);

        logger.info("ID {}, TS {}", transContext1.getTransId(), transContext1.getTimestamp());
        logger.info("ID {}, TS {}", transContext2.getTransId(), transContext2.getTimestamp());
        transService.commitTrans(transContext2.getTransId(), false);

        transContext = transService.beginTrans(true);
        logger.info("ID {}, TS {}", transContext.getTransId(), transContext.getTimestamp());


    }
}
