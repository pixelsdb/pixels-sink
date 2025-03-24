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
package io.pixelsdb.pixels.sink;

import org.junit.Test;

/**
 * Created at: 29/04/2021
 * Author: hank
 */
public class TestSplitString
{
    @Test
    public void test()
    {
        String s = "1|3689999|O|224560.83|1996-01-02|5-LOW|Clerk#000095055|0|nstructions sleep furiously among |";
        String reg = "\\\\|";
        String[] splits = s.split(reg);
        System.out.println(splits.length);
    }
}
