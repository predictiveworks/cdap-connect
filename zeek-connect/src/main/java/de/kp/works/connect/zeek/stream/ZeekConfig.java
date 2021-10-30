package de.kp.works.connect.zeek.stream;
/*
 * Copyright (c) 2019 - 2021 Dr. Krusche & Partner PartG. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 * @author Stefan Krusche, Dr. Krusche & Partner PartG
 *
 */

import com.google.common.base.Strings;
import de.kp.works.connect.common.BaseConfig;
import de.kp.works.stream.zeek.ZeekNames;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;

import javax.annotation.Nullable;
import java.util.Properties;

public class ZeekConfig extends BaseConfig {

    @Description("The file system folder used by the Zeek sensor to persist log files.")
    @Macro
    public String zeekFolder;

    @Description("The log file extension used by the Zeek sensor. Default value is 'log'.")
    @Macro
    @Nullable
    public String zeekExtension;

    @Description("The buffer size used to cache Zeek log entries. Default value is '1000'.")
    @Macro
    @Nullable
    public String zeekBufferSize;

    @Description("The maximum number of bytes of a Zeek log entry. Default value is '8192'.")
    @Macro
    @Nullable
    public String zeekLineSize;

    @Description("The number of threads used to process Zeek log entries. Default value is '1'.")
    @Macro
    @Nullable
    public String zeekNumThreads;

    @Description("The polling interval in seconds to retrieve Zeek log entries. Default value is '1'.")
    @Macro
    @Nullable
    public String zeekPolling;

    public void validate() {

        String className = this.getClass().getName();

        if (Strings.isNullOrEmpty(zeekFolder)) {
            throw new IllegalArgumentException(
                    String.format("[%s] The Zeek log folder must not be empty.", className));
        }

    }

    public Properties toProperties() {

        Properties props = new Properties();
        props.setProperty(ZeekNames.LOG_FOLDER(), zeekFolder);

        if (!Strings.isNullOrEmpty(zeekExtension)) {
            props.setProperty(ZeekNames.LOG_POSTFIX(), zeekExtension);
        }

        if (!Strings.isNullOrEmpty(zeekBufferSize)) {
            props.setProperty(ZeekNames.MAX_BUFFER_SIZE(), zeekBufferSize);
        }

        if (!Strings.isNullOrEmpty(zeekLineSize)) {
            props.setProperty(ZeekNames.MAX_LINE_SIZE(), zeekLineSize);
        }

        if (!Strings.isNullOrEmpty(zeekNumThreads)) {
            props.setProperty(ZeekNames.NUM_THREADS(), zeekNumThreads);
        }

        if (!Strings.isNullOrEmpty(zeekPolling)) {
            props.setProperty(ZeekNames.POLLING_INTERVAL(), zeekPolling);
        }

        return props;
    }

}
