/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.irc;

import org.apache.camel.AsyncProcessor;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.processor.CamelInternalProcessor;
import org.apache.camel.processor.Pipeline;
import org.apache.camel.processor.PipelineExchangeAdjuster;
import org.apache.camel.processor.SendProcessor;
import org.apache.camel.processor.interceptor.DefaultChannel;

/**
 * Wrapper for {@link Processor} classes used with IRC.
 */
public class IrcProcessor implements Processor {

    private final Processor processor;


    IrcProcessor(Processor processor) {
        this.processor = processor;
        if (processor instanceof CamelInternalProcessor) {
            AsyncProcessor nextProcessor = ((CamelInternalProcessor) processor).getProcessor();
            if (nextProcessor instanceof Pipeline) {
                Pipeline pipeline = (Pipeline) nextProcessor;
                pipeline.setExchangeAdjuster(new PipelineExchangeAdjuster() {

                    @Override
                    public void adjustExchange(Exchange exchange, Processor processor) {
                        if (processor instanceof DefaultChannel) {
                            Processor nextProcessor = ((DefaultChannel) processor).getNextProcessor();

                            if (nextProcessor instanceof SendProcessor) {
                                SendProcessor sendProcessor = (SendProcessor) nextProcessor;
                                if (sendProcessor.getDestination() instanceof IrcEndpoint) {
                                    IrcEndpoint endpoint = (IrcEndpoint) sendProcessor.getDestination();
                                    String newTarget = endpoint.getConfiguration().getUsername();
                                    if (exchange.getIn() instanceof IrcMessage) {
                                        IrcMessage message = (IrcMessage) exchange.getIn();
                                        message.setTarget(newTarget);
                                        message.setHeader("irc.target", newTarget);
                                    }
                                }
                            }
                        }
                    }

                });
            }
        }
    }


    @Override
    public void process(Exchange exchange) throws Exception {
        processor.process(exchange);
    }

}
