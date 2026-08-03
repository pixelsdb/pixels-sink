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
package io.pixelsdb.pixels.sink.source.kafka;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.PixelsSinkConstants;
import io.pixelsdb.pixels.sink.config.factory.KafkaPropFactorySelector;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;
import io.pixelsdb.pixels.sink.pipeline.TablePipelineManager;
import io.pixelsdb.pixels.sink.pipeline.TransactionPipeline;
import io.pixelsdb.pixels.sink.source.SinkSource;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SinkKafkaSource implements SinkSource
{
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 10;

    private ExecutorService sourceExecutor;
    private KafkaTransactionSource transactionSource;
    private TopicProcessor topicProcessor;
    private TablePipelineManager tablePipelineManager;
    private TransactionPipeline transactionPipeline;
    private volatile boolean running;

    @Override
    public void start()
    {
        PixelsSinkConfig pixelsSinkConfig = PixelsSinkConfigFactory.getInstance();
        KafkaPropFactorySelector kafkaPropFactorySelector = new KafkaPropFactorySelector();

        Properties transactionKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.TRANSACTION_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        String transactionTopic = pixelsSinkConfig.getTopicPrefix() + "." +
                pixelsSinkConfig.getTransactionTopicSuffix();
        tablePipelineManager = new TablePipelineManager();
        transactionPipeline = new TransactionPipeline();
        transactionSource =
                new KafkaTransactionSource(
                        transactionKafkaProperties, transactionTopic, transactionPipeline);

        Properties topicKafkaProperties = kafkaPropFactorySelector
                .getFactory(PixelsSinkConstants.ROW_RECORD_KAFKA_PROP_FACTORY)
                .createKafkaProperties(pixelsSinkConfig);
        topicProcessor = new TopicProcessor(
                pixelsSinkConfig, topicKafkaProperties, tablePipelineManager);

        transactionPipeline.start();
        sourceExecutor = Executors.newFixedThreadPool(2);
        sourceExecutor.submit(transactionSource);
        sourceExecutor.submit(topicProcessor);
        running = true;
    }


    @Override
    public void close()
    {
        if (transactionSource != null)
        {
            transactionSource.requestStop();
        }
        if (topicProcessor != null)
        {
            topicProcessor.requestStop();
        }

        boolean sourcesStopped = true;
        if (sourceExecutor != null)
        {
            sourceExecutor.shutdown();
            try
            {
                if (!sourceExecutor.awaitTermination(
                        SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS))
                {
                    sourceExecutor.shutdownNow();
                    sourcesStopped = false;
                }
            } catch (InterruptedException e)
            {
                sourceExecutor.shutdownNow();
                Thread.currentThread().interrupt();
                sourcesStopped = false;
            }
        }

        if (transactionPipeline != null)
        {
            if (sourcesStopped)
            {
                transactionPipeline.close();
            } else
            {
                transactionPipeline.abort();
            }
        }
        if (tablePipelineManager != null)
        {
            if (sourcesStopped)
            {
                tablePipelineManager.close();
            } else
            {
                tablePipelineManager.abort();
            }
        }
        running = false;
    }

    @Override
    public void abort()
    {
        running = false;
        if (transactionSource != null)
        {
            transactionSource.requestStop();
        }
        if (topicProcessor != null)
        {
            topicProcessor.abort();
        }
        if (sourceExecutor != null)
        {
            sourceExecutor.shutdownNow();
        }
        if (transactionPipeline != null)
        {
            transactionPipeline.abort();
        }
        if (tablePipelineManager != null)
        {
            tablePipelineManager.abort();
        }
    }

    @Override
    public boolean isRunning()
    {
        return running;
    }
}
