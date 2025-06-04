package io.pixelsdb.pixels.sink.concurrent;


import io.pixelsdb.pixels.common.exception.TransException;
import io.pixelsdb.pixels.common.transaction.TransContext;
import io.pixelsdb.pixels.common.transaction.TransService;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;

public class TransactionServiceTest {

    @Test
    public void testTransactionService() {
        int numTransactions = 10;

        TransService transService = TransService.CreateInstance("localhost", 18889);
        try {
            List<TransContext> transContexts =  transService.beginTransBatch(numTransactions, false);
            assertEquals(numTransactions, transContexts.size());
            TransContext prevTransContext = transContexts.get(0);
            for(int i = 1; i < numTransactions; i++) {
                TransContext transContext = transContexts.get(i);
                assertTrue(transContext.getTransId() > prevTransContext.getTransId());
                assertTrue(transContext.getTimestamp() > prevTransContext.getTimestamp());
                prevTransContext = transContext;
            }
        } catch (TransException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testBatchRequest() {
        int numTransactions = 1000;

        TransService transService = TransService.CreateInstance("localhost", 18889);
        try {
            List<TransContext> transContexts =  transService.beginTransBatch(numTransactions, false);
            assertEquals(numTransactions, transContexts.size());
            TransContext prevTransContext = transContexts.get(0);
            for(int i = 1; i < numTransactions; i++) {
                TransContext transContext = transContexts.get(i);
                assertTrue(transContext.getTransId() > prevTransContext.getTransId());
                assertTrue(transContext.getTimestamp() > prevTransContext.getTimestamp());
                prevTransContext = transContext;
            }
        } catch (TransException e) {
            throw new RuntimeException(e);
        }
    }
}
