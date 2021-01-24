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
import org.apache.flink.runtime.rest.handler.util.HandlerUtils;
import org.apache.flink.runtime.rest.messages.ErrorResponseBody;

import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpHeaders;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpMessage;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpObject;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpRequest;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;
import org.apache.flink.shaded.netty4.io.netty.util.ReferenceCountUtil;

import java.util.HashMap;

/**
 * This is the last handler in the pipeline. It logs all error messages and sends exception
 * responses.
 */
@ChannelHandler.Sharable
public class BasicAuthHandler extends SimpleChannelInboundHandler<HttpObject> {
    private final BasicAuth auth;

    private BasicAuthHandler(BasicAuth auth) {
        this.auth = auth;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject message) {
        if (message instanceof HttpRequest) {
            String header =
                    HttpHeaders.getHeader((HttpMessage) message, HttpHeaders.Names.AUTHORIZATION);
            if (!auth.getAuthorization().equals(header)) {
                sendAuthRequest(ctx, (HttpRequest) message);
                return;
            }
        }
        ctx.fireChannelRead(ReferenceCountUtil.retain(message));
    }

    private void sendAuthRequest(ChannelHandlerContext ctx, HttpRequest message) {
        HashMap<String, String> headers = new HashMap<>();
        headers.put(HttpHeaders.Names.WWW_AUTHENTICATE, auth.getAuthenticate());
        HandlerUtils.sendErrorResponse(
                ctx,
                message,
                new ErrorResponseBody("Authorization Required"),
                HttpResponseStatus.UNAUTHORIZED,
                headers);
    }

    public static BasicAuthHandler fromConfiguration(
            Configuration config,
            ConfigOption<String> realmOption,
            ConfigOption<String> userNameOption,
            ConfigOption<String> passwordOption) {
        BasicAuth basicAuth =
                BasicAuth.fromConfiguration(config, realmOption, userNameOption, passwordOption);
        if (basicAuth != null) {
            return new BasicAuthHandler(basicAuth);
        }
        return null;
    }
}
