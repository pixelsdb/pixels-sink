/*
 * Copyright 2018-2019 PixelsDB.
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
package io.pixelsdb.pixels.sink;

import org.apache.hadoop.fs.Path;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @Description: souce -> BlockingQueue
 * @author: tao
 * @author hank 
 * @date: Create in 2018-10-30 12:57
 **/
public class PixelsProducer
{
    private static PixelsProducer instance = new PixelsProducer();

    public PixelsProducer()
    {
    }

    public static PixelsProducer getInstance()
    {
        return instance;
    }

    public void startProducer(BlockingQueue<Path> queue)
    {
    }

    class Producer extends Thread
    {
        private volatile boolean isRunning = true;
        private BlockingQueue<Path> queue;
        Path data = null;

        public Producer(BlockingQueue<Path> queue)
        {
            this.queue = queue;
        }

        @Override
        public void run()
        {
            System.out.println("start producer thread！");
            try
            {
                while (isRunning)
                {
                    System.out.println("begin to generate data...");

                    System.out.println("add：" + data + "into queue...");
                    if (!queue.offer(data, 2, TimeUnit.SECONDS))
                    {
                        System.out.println("add error：" + data);
                    }
                }
            } catch (InterruptedException e)
            {
                e.printStackTrace();
                Thread.currentThread().interrupt();
            } finally
            {
                System.out.println("Exit producer thread！");
            }
        }

        public void stopProducer()
        {
            isRunning = false;
        }
    }

}
