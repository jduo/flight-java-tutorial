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

import static com.google.common.collect.Streams.stream;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.flight.Action;
import org.apache.arrow.flight.ActionType;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.Criteria;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightEndpoint;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.FlightProducer;
import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.PutResult;
import org.apache.arrow.flight.Result;
import org.apache.arrow.flight.Ticket;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorLoader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.Schema;

/**
 * Example Arrow Flight server. This takes in resource names of parquet files in getFlightInfo requests
 * and serves the parquet file in getStream().
 *
 * The ticket content should be the absolute path of the parquet file.
 */
public class DemoFlightProducer implements FlightProducer {

  private final BufferAllocator allocator;
  private final int port;
  public DemoFlightProducer(BufferAllocator allocator, int port) {
    this.allocator = allocator;
    this.port = port;
  }

  @Override
  public void getStream(CallContext callContext, Ticket ticket, ServerStreamListener serverStreamListener) {
    try {
      final String rawPath = new String(ticket.getBytes(), StandardCharsets.UTF_8);

      // Use the Dataset API to read the parquet file.
      // This code is an example of preparing a vector but isn't related to Flight.
      DatasetFactory factory = new FileSystemDatasetFactory(allocator,
          NativeMemoryPool.getDefault(), FileFormat.PARQUET, rawPath);
      Dataset dataset = factory.finish();
      Scanner scanner = dataset.newScan(new ScanOptions(100));
      List<ArrowRecordBatch> batches = StreamSupport.stream(
              scanner.scan().spliterator(), false)
          .flatMap(t -> stream(t.execute()))
          .collect(Collectors.toList());

      // Migrate the ArrowRecordBatches to a VectorSchemaRoot using the vector loader.
      VectorSchemaRoot root = VectorSchemaRoot.create(factory.inspect(), allocator);
      VectorLoader loader = new VectorLoader(root);
      serverStreamListener.start(root);
      batches.forEach(batch -> {
        loader.load(batch);
        serverStreamListener.putNext(); // Notify the client of new data.
          });
      serverStreamListener.completed(); // Notify the client that all data ise sent.

      // Clean-up. Really should use try-with-resources.
      AutoCloseables.close(batches);
      AutoCloseables.close(factory, dataset, scanner);
      AutoCloseables.close(root);
    } catch (Exception e) {
      // All errors must be sent back to the client using this method, or else
      // the client will hang.
      serverStreamListener.error(e);
    }
  }

  @Override
  public void listFlights(CallContext callContext, Criteria criteria, StreamListener<FlightInfo> streamListener) {
    try {
      streamListener.onNext(resourceNameToFlightInfo("SF_incidents2016.parquet", null));
    } catch (Exception ex) {
      streamListener.onError(ex);
    }
  }

  @Override
  public FlightInfo getFlightInfo(CallContext callContext, FlightDescriptor flightDescriptor) {
    try {
      final String request = new String(flightDescriptor.getCommand(), StandardCharsets.UTF_8);
      return resourceNameToFlightInfo(request, flightDescriptor);
    } catch (Exception e) {
      throw CallStatus.UNKNOWN
          .withCause(e)
          .toRuntimeException();
    }
  }

  @Override
  public Runnable acceptPut(CallContext callContext, FlightStream flightStream, StreamListener<PutResult> streamListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void doAction(CallContext callContext, Action action, StreamListener<Result> streamListener) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void listActions(CallContext callContext, StreamListener<ActionType> streamListener) {
    throw new UnsupportedOperationException();
  }

  private String resourceNameToURIString(String resourceName) throws Exception {
    final URL resourceUri = this.getClass().getResource("/" + resourceName);
    return resourceUri.toURI().toString();
  }

  private FlightInfo resourceNameToFlightInfo(String resourceName, FlightDescriptor flightDescriptor) throws Exception {
    final String resourceUri = resourceNameToURIString(resourceName);
    DatasetFactory factory = new FileSystemDatasetFactory(allocator,
        NativeMemoryPool.getDefault(), FileFormat.PARQUET, resourceNameToURIString(resourceName));

    Schema schema = factory.inspect();
    final FlightInfo result = new FlightInfo(schema, flightDescriptor, Arrays.asList(
        new FlightEndpoint(
            new Ticket(resourceUri.getBytes(StandardCharsets.UTF_8)),
            Location.forGrpcInsecure("localhost", port))), -1, -1);
    factory.close();
    return result;
  }
}
