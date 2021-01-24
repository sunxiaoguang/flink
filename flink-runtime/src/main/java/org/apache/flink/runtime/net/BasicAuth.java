/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.net;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Preconditions;

import java.util.Base64;

/** Basic authentication. */
public class BasicAuth {
    private final String authorization;
    private final String wwwAuthenticate;
    private static Base64.Encoder encoder = Base64.getEncoder();

    private BasicAuth(String realm, String userName, String password) {
        this.authorization =
                "Basic " + encoder.encodeToString((userName + ":" + password).getBytes());
        this.wwwAuthenticate = "Basic realm=\"" + realm + "\", charset=\"UTF-8\"";
    }

    public String getAuthorization() {
        return authorization;
    }

    public String getAuthenticate() {
        return wwwAuthenticate;
    }

    public static BasicAuth fromConfiguration(
            Configuration config,
            ConfigOption<String> realmOption,
            ConfigOption<String> userNameOption,
            ConfigOption<String> passwordOption) {
        final String realm = config.getString(realmOption);
        if (realm == null) {
            return null;
        }
        final String userName =
                Preconditions.checkNotNull(
                        config.getString(userNameOption), "userName can not be empty");
        final String password =
                Preconditions.checkNotNull(
                        config.getString(passwordOption), "password can not be empty");
        return new BasicAuth(realm, userName, password);
    }
}
