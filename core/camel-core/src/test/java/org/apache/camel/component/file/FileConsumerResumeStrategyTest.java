/*
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
package org.apache.camel.component.file;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.camel.ContextTestSupport;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.file.consumer.DirectoryEntriesResumeAdapter;
import org.apache.camel.component.file.consumer.FileResumeAdapter;
import org.apache.camel.component.file.consumer.adapters.DirectoryEntries;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.processor.resume.TransientResumeStrategy;
import org.apache.camel.support.resume.Resumables;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("Tests whether file consumer works with the resume strategy")
public class FileConsumerResumeStrategyTest extends ContextTestSupport {

    private static class TestFileSetResumeAdapter implements FileResumeAdapter, DirectoryEntriesResumeAdapter {
        private final List<Path> processedFiles = Arrays.asList(Path.of("0.txt"), Path.of("1.txt"), Path.of("2.txt"));
        private DirectoryEntries resumeSet;

        @Override
        public void setResumePayload(DirectoryEntries resumeSet) {
            this.resumeSet = Objects.requireNonNull(resumeSet);
        }

        @Override
        public void resume() {
            DirectoryEntries.doResume(resumeSet, path -> !processedFiles.contains(path.getFileName()));
        }

    }

    private final TestFileSetResumeAdapter adapter = new TestFileSetResumeAdapter();

    private static Map<String, Object> headerFor(int num) {
        String name = num + ".txt";

        return Map.of(Exchange.FILE_NAME, name);
    }

    @DisplayName("Tests whether it can resume processing of directory entries")
    @Test
    public void testResume() throws Exception {
        MockEndpoint mock = getMockEndpoint("mock:result");
        mock.expectedBodiesReceivedInAnyOrder("3", "4", "5", "6");

        template.sendBodyAndHeaders(fileUri("resume"), "0", headerFor(0));
        template.sendBodyAndHeaders(fileUri("resume"), "1", headerFor(1));
        template.sendBodyAndHeaders(fileUri("resume"), "2", headerFor(2));
        template.sendBodyAndHeaders(fileUri("resume"), "3", headerFor(3));
        template.sendBodyAndHeaders(fileUri("resume"), "4", headerFor(4));
        template.sendBodyAndHeaders(fileUri("resume"), "5", headerFor(5));
        template.sendBodyAndHeaders(fileUri("resume"), "6", headerFor(6));

        // only expect 4 of the 6 sent
        assertMockEndpointsSatisfied();

        assertEquals(4, adapter.resumeSet.resumed().count(), "There should be exactly 4 resumables");
    }

    private void setOffset(Exchange exchange) {
        String body = exchange.getMessage().getBody(String.class);

        if (body != null) {
            Integer num = Integer.valueOf(body);
            exchange.getMessage().setHeader(Exchange.OFFSET, Resumables.of(body + ".txt", num));
        }
    }

    @Override
    protected RouteBuilder createRouteBuilder() {
        return new RouteBuilder() {
            @Override
            public void configure() throws Exception {

                bindToRegistry("testResumeStrategy", new TransientResumeStrategy(adapter));

                from(fileUri("resume?noop=true&recursive=true"))
                        .resumable("testResumeStrategy")
                        .process(e -> setOffset(e))
                        .convertBodyTo(String.class)
                        .to("mock:result");
            }
        };
    }

}
