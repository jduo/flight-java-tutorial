/*
 *     Copyright 2022 James Duong
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.example;

import java.nio.charset.StandardCharsets;

import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightServer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

public class DemoApplication implements AutoCloseable {

  public static void main(String[] args) throws Exception {
    final int port = 30000;
    try (DemoApplication application = new DemoApplication(port)) {
      try (FlightClient client = FlightClient.builder()
          .allocator(application.allocator)
          .location(Location.forGrpcInsecure("localhost", port))
          .build()) {
        final FlightInfo flightInfo = client.getInfo(toFlightDescriptor("SF_incidents2016.parquet"));
        System.out.println("Schema:" + flightInfo.getSchema().toString());
        // Assume the original endpoint that we issued getFlightInfo() on is one of the endpoints that
        // holds data for the ticket. Also assume only one ticket.
        try (FlightStream stream = client.getStream(flightInfo.getEndpoints().get(0).getTicket())) {
          while (stream.next()) {
            System.out.println(stream.getRoot().contentToTSVString());
          }
        }
      }
    }
  }

  private final FlightServer flightServer;
  private final BufferAllocator allocator;

  public DemoApplication(int port) throws Exception {
    allocator = new RootAllocator(Long.MAX_VALUE);
    flightServer = FlightServer.builder()
        .allocator(allocator)
        .producer(new DemoFlightProducer(allocator, port))
        .location(Location.forGrpcInsecure("localhost", port))
        .build();
    flightServer.start();
  }

  @Override
  public void close() throws Exception {
    flightServer.close();
    allocator.close();
  }

  private static FlightDescriptor toFlightDescriptor(String resourceName) {
    return FlightDescriptor.command(resourceName.getBytes(StandardCharsets.UTF_8));
  }
}
