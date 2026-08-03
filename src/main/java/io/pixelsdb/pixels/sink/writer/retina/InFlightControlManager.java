/*
 * Copyright 2026 PixelsDB.
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
package io.pixelsdb.pixels.sink.writer.retina;

import io.pixelsdb.pixels.sink.config.PixelsSinkConfig;
import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.concurrent.Semaphore;

public class InFlightControlManager
{

    private static volatile InFlightControlManager instance;
    private final Semaphore semaphore;

    private InFlightControlManager()
    {
        PixelsSinkConfig config = PixelsSinkConfigFactory.getInstance();
        int MAX_IN_FLIGHT = config.getRetinaRpcLimit();
        this.semaphore = new Semaphore(MAX_IN_FLIGHT);
    }

    public static InFlightControlManager getInstance()
    {
        if (instance == null)
        {
            synchronized (InFlightControlManager.class)
            {
                if (instance == null)
                {
                    instance = new InFlightControlManager();
                }
            }
        }
        return instance;
    }

    public void acquire(int permits)
    {
        try
        {
            semaphore.acquire(permits);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void release(int permits)
    {
        semaphore.release(permits);
    }
}
