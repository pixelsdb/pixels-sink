/*
 * Copyright 2019 PixelsDB.
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.junit.Test;

import java.io.IOException;

public class TestOrcReader
{

    String orcPath = "hdfs://dbiir10:9000/pixels/pixels/test_105/orc/000014_0";

    @Test
    public void testReader()
    {
        Configuration conf = new Configuration();
        Reader reader = null;
        try
        {
            reader = OrcFile.createReader(new Path(orcPath),
                    OrcFile.readerOptions(conf));
            RecordReader rows = null;
            rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            long num = 0;
            long begin = System.currentTimeMillis();
            while (rows.nextBatch(batch))
            {
                num += batch.size;
            }
            long end = System.currentTimeMillis();
            System.out.println("Size: " + num);
            System.out.println("Time: " + (end - begin));
            System.out.println(reader.getRawDataSize());
            System.out.println(reader.getStripes().size());
            System.out.println(reader.getStripes().get(0).getLength());
            System.out.println(reader.getStripes().get(0).getDataLength());
            System.out.println(reader.getStripes().get(0).getFooterLength());
            System.out.println(reader.getStripes().get(0).getNumberOfRows());

            System.out.println(reader.getStripes().get(1).getLength());
            System.out.println(reader.getStripes().get(1).getDataLength());
            System.out.println(reader.getStripes().get(1).getFooterLength());
            System.out.println(reader.getStripes().get(1).getNumberOfRows());
            rows.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Test
    public void testPixelWriterByOrcReader()
    {
        Configuration conf = new Configuration();
        Reader reader = null;
        try
        {
            reader = OrcFile.createReader(new Path(TestParams.orcPath),
                    OrcFile.readerOptions(conf));
            RecordReader rows = null;
            rows = reader.rows();
            VectorizedRowBatch batch = reader.getSchema().createRowBatch();
            long num = 0;
            long begin = System.currentTimeMillis();
            while (rows.nextBatch(batch))
            {
                num += batch.size;
//                System.out.println(batch.toString());
                int i, j, k;
                StringBuilder b = new StringBuilder();
                for (i = 0; i < batch.size; ++i)
                {
                    for (k = 0; k < batch.projectionSize; ++k)
                    {
                        int projIndex = batch.projectedColumns[k];
                        ColumnVector cv = batch.cols[projIndex];
                        cv.stringifyValue(b, i);
                    }
                    System.out.println(b.toString());
                    if (i == 1)
                    {
                        break;
                    }
                }
                break;
            }
            long end = System.currentTimeMillis();
            System.out.println("Size: " + num);
            System.out.println("Time: " + (end - begin));
            rows.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }
}
