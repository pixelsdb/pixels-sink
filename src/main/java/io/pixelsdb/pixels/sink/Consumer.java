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

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

public abstract class Consumer extends Thread
{

    protected Consumer()
    {
    }

    private BlockingQueue<Path> queue;
    private Properties prop;
    private Config config;

    public Properties getProp()
    {
        return prop;
    }

    public Consumer(BlockingQueue<Path> queue, Properties prop, Config config)
    {
        this.queue = queue;
        this.prop = prop;
        this.config = config;
    }

}
