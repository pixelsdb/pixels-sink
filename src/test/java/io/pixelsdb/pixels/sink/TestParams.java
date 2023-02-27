/*
 * Copyright 2023 PixelsDB.
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

/**
 * Created at: 2/12/23
 * Author: hank
 */
public class TestParams
{
    public static String orcPath = "/path/to/orc/file";
    public static String schemaStr = "struct<a:int,b:float,c:double,d:timestamp,e:boolean,f:string>";
    public static int pixelStride = 10000;
    public static long blockSize = 2048L*1024L*1024L;
}
