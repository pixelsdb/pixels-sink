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

package io.pixelsdb.pixels.sink.util;

import io.pixelsdb.pixels.sink.config.factory.PixelsSinkConfigFactory;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

public class LatencySimulator {
    private static final Random RANDOM = ThreadLocalRandom.current();
    private static final double longTailProb = 0.01;
    private static final double longTailScale = 30;
    private static final double tailVariance = 0.1;
    private static final double normalVariance = 0.4;

    private static long generateLongTailDelay(long baseDelayMs) {
        if (RANDOM.nextDouble() < longTailProb) {
            double variance = 1 + (RANDOM.nextDouble() * 2 - 1) * tailVariance;
            return (long) (baseDelayMs * longTailScale * variance);
        }

        return (long) (baseDelayMs * (1 + (RANDOM.nextDouble() - 0.5) * normalVariance));
    }

    public static void smartDelay() {
        try {
            TimeUnit.MILLISECONDS.sleep(generateLongTailDelay(PixelsSinkConfigFactory.getInstance().getMockRpcDelay()));
        } catch (InterruptedException ignored) {

        }
    }
}