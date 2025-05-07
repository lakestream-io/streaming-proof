/*
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
package io.streamnative.streaming.proof.coordinator;

import io.javalin.Javalin;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/**
 * Main entry point for the Streaming Proof Coordinator service.
 * This class initializes and manages the coordinator service, providing
 * command-line interface for configuration and lifecycle management.
 *
 * <p>The coordinator starter:
 * <ul>
 *   <li>Handles command-line arguments for service configuration</li>
 *   <li>Initializes the HTTP server and coordinator components</li>
 *   <li>Manages the service lifecycle (start/stop)</li>
 * </ul>
 *
 * <p>Example usage:
 * <pre>{@code
 * // Start with default port (8080)
 * java -jar coordinator.jar
 *
 * // Start with custom port
 * java -jar coordinator.jar -p 9090
 * }</pre>
 *
 * @see Coordinator
 * @see CoordinatorHandler
 */
@Slf4j
@Command(name = "Streaming proof coordinator starter", description = "Start a streaming proof coordinator.")
public class CoordinatorStarter implements Callable<Integer> {

    /** HTTP port for the coordinator service to listen on */
    @Option(names = {"-p", "--port"}, description = "HTTP port to listen on.")
    private int httpPort = 8080;

    /** Javalin HTTP server instance */
    private Javalin app;
    
    /** The coordinator instance that manages proof tests */
    private final Coordinator coordinator = new Coordinator();

    /**
     * Executes the coordinator service startup.
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
     * Starts the coordinator service on the specified port.
     * Initializes the HTTP server and sets up request handlers.
     *
     * @param port The HTTP port to listen on
     */
    public void start(int port) {
        log.info("Starting coordinator on port {}", port);
        this.app = Javalin.create();
        new CoordinatorHandler(app, coordinator);
        app.start(port);
        log.info("Coordinator started successfully");
    }

    /**
     * Gracefully stops the coordinator service.
     * Shuts down the HTTP server and cleans up resources.
     */
    public void stop() {
        log.info("Stopping coordinator");
        app.stop();
        coordinator.close();
        log.info("Stopped coordinator");
    }

    /**
     * Main entry point for the coordinator service.
     * Processes command-line arguments and starts the service.
     *
     * @param args Command-line arguments
     */
    public static void main(String[] args) {
        new CommandLine(new CoordinatorStarter()).execute(args);
    }
}