/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.streamnative.streaming.proof.worker;

import io.javalin.Javalin;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Main entry point for the Streaming Proof Worker service.
 * This class initializes and manages a worker node that handles producer and consumer
 * operations for streaming proof tests.
 *
 * <p>The worker starter:
 * <ul>
 *   <li>Handles command-line arguments for service configuration</li>
 *   <li>Initializes the HTTP server and worker components</li>
 *   <li>Exposes REST API endpoints through {@link WorkerHandler}</li>
 * </ul>
 *
 * <p>The worker node can manage multiple producer and consumer groups concurrently,
 * each identified by a unique proof ID. It communicates with the coordinator service
 * to participate in distributed streaming proof tests.
 *
 * <p>Example usage:
 * <pre>{@code
 * // Start with default port (8088)
 * java -jar worker.jar
 *
 * // Start with custom port
 * java -jar worker.jar -p 9090
 *
 * // Start programmatically
 * WorkerStarter starter = new WorkerStarter();
 * starter.start(8088);
 * }</pre>
 *
 * @see Worker
 * @see WorkerHandler
 */
@Slf4j
@Command(name = "Streaming proof worker starter", description = "Start a streaming proof worker.")
public class WorkerStarter implements Callable<Integer> {

    /** HTTP port for the worker service to listen on */
    @Option(names = {"-p", "--port"}, description = "HTTP port to listen on.")
    private int httpPort = 8088;

    /** Javalin HTTP server instance */
    private Javalin app;

    /** Worker instance that manages producer and consumer operations.
     * This field maintains the core worker functionality for handling streaming proof tests.
     * It is initialized during startup and passed to {@link WorkerHandler} for REST endpoint handling.
     * The worker instance is closed during service shutdown to ensure proper resource cleanup.
     *
     * @see Worker
     * @see WorkerHandler
     */
    Worker worker;

    /**
     * Executes the worker service startup.
     * This method is called by the picocli framework when the command is run.
     *
     * @return 0 if the service starts successfully
     */
    @Override
    public Integer call() {
        start(httpPort);
        return 0;
    }

    /**
     * Starts the worker service on the specified port.
     * Initializes the HTTP server and sets up request handlers.
     *
     * @param port The HTTP port to listen on
     */
    public void start(int port) {
        log.info("Starting worker on port {}", port);
        app = Javalin.create();
        worker = new Worker();
        new WorkerHandler(app, worker);
        app.start(port);
        log.info("port started successfully");
    }

    /**
     * Gracefully stops the coordinator service.
     * Shuts down the HTTP server and cleans up resources.
     */
    public void stop() {
        log.info("Stopping coordinator");
        app.stop();
        worker.close();
        log.info("Stopped coordinator");
    }

    /**
     * Main entry point for starting the worker service from the command line.
     * Uses picocli to parse command-line arguments and execute the service.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        new CommandLine(new WorkerStarter()).execute(args);
    }
}