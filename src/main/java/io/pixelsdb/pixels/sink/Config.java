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

import com.alibaba.fastjson.JSON;
import io.pixelsdb.pixels.common.exception.MetadataException;
import io.pixelsdb.pixels.common.metadata.MetadataService;
import io.pixelsdb.pixels.common.metadata.domain.Column;
import io.pixelsdb.pixels.common.metadata.domain.Layout;
import io.pixelsdb.pixels.common.metadata.domain.Order;
import io.pixelsdb.pixels.common.utils.ConfigFactory;

import java.util.Arrays;
import java.util.List;

public class Config
{
    private final String dbName;
    private final String tableName;
    private final int maxRowNum;
    private final String regex;
    private final String format;
    private String pixelsPath;
    private String schema;
    private int[] orderMapping;
    private final boolean enableEncoding;

    public String getPixelsPath()
    {
        return pixelsPath;
    }

    public String getSchema()
    {
        return schema;
    }

    public int[] getOrderMapping()
    {
        return orderMapping;
    }

    public int getMaxRowNum()
    {
        return maxRowNum;
    }

    public String getRegex()
    {
        return regex;
    }

    public String getFormat()
    {
        return format;
    }

    public boolean isEnableEncoding()
    {
        return enableEncoding;
    }

    public Config(String dbName, String tableName, int maxRowNum, String regex, String format, String pixelsPath, boolean enableEncoding)
    {
        this.dbName = dbName;
        this.tableName = tableName;
        this.maxRowNum = maxRowNum;
        this.regex = regex;
        this.format = format;
        this.pixelsPath = pixelsPath;
        this.enableEncoding = enableEncoding;
    }

    public boolean load(ConfigFactory configFactory) throws MetadataException, InterruptedException
    {
        // init metadata service
        String metaHost = configFactory.getProperty("metadata.server.host");
        int metaPort = Integer.parseInt(configFactory.getProperty("metadata.server.port"));
        MetadataService metadataService = new MetadataService(metaHost, metaPort);
        // get columns of the specified table
        List<Column> columns = metadataService.getColumns(dbName, tableName, false);
        int colSize = columns.size();
        // record original column names and types
        String[] originalColNames = new String[colSize];
        String[] originalColTypes = new String[colSize];
        for (int i = 0; i < colSize; i++)
        {
            originalColNames[i] = columns.get(i).getName();
            originalColTypes[i] = columns.get(i).getType();
        }
        // get the latest layout for writing
        List<Layout> layouts = metadataService.getLayouts(dbName, tableName);
        Layout writingLayout = null;
        int writingLayoutVersion = -1;
        for (Layout layout : layouts)
        {
            if (layout.isWritable())
            {
                if (layout.getVersion() > writingLayoutVersion)
                {
                    writingLayout = layout;
                    writingLayoutVersion = layout.getVersion();
                }
            }
        }
        // no layouts for writing currently
        if (writingLayout == null)
        {
            return false;
        }
        // get the column order of the latest writing layout
        Order order = JSON.parseObject(writingLayout.getOrder(), Order.class);
        List<String> layoutColumnOrder = order.getColumnOrder();
        // check size consistency
        if (layoutColumnOrder.size() != colSize)
        {
            return false;
        }
        // map the column order of the latest writing layout to the original column order
        int[] orderMapping = new int[colSize];
        List<String> originalColNameList = Arrays.asList(originalColNames);
        for (int i = 0; i < colSize; i++)
        {
            int index = originalColNameList.indexOf(layoutColumnOrder.get(i));
            if (index >= 0)
            {
                orderMapping[i] = index;
            } else
            {
                return false;
            }
        }
        // construct pixels schema based on the column order of the latest writing layout
        StringBuilder schemaBuilder = new StringBuilder("struct<");
        for (int i = 0; i < colSize; i++)
        {
            String name = layoutColumnOrder.get(i);
            String type = originalColTypes[orderMapping[i]];
            /**
             * Issue #100:
             * Refer TypeDescription, ColumnReader, and ColumnWriter for how Pixels
             * deals with data types.
             */
            schemaBuilder.append(name).append(":").append(type)
                    .append(",");
        }
        schemaBuilder.replace(schemaBuilder.length() - 1, schemaBuilder.length(), ">");

        // get path of loading
        if(this.pixelsPath == null)
        {
            String loadingDataPath = writingLayout.getOrderPath();
            if (!loadingDataPath.endsWith("/"))
            {
                loadingDataPath += "/";
            }
            this.pixelsPath = loadingDataPath;
        }
        else
        {
            if (!this.pixelsPath.endsWith("/"))
            {
                this.pixelsPath += "/";
            }
        }
        // init the params
        this.schema = schemaBuilder.toString();
        this.orderMapping = orderMapping;
        metadataService.shutdown();
        return true;
    }

}
