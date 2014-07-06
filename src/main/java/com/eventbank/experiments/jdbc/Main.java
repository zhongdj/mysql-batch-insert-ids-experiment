package com.eventbank.experiments.jdbc;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class Main {
    public static void main(String[] args) throws InterruptedException {
        final String user = "root";
        final String pass = "1q2w3e4r5t";
        final String url = "jdbc:mysql://127.0.0.1:3306/insert_test";

        final ExecutorService service = Executors.newFixedThreadPool(5);
        final BlockingQueue<ValidationResult> resultQueue = new LinkedBlockingQueue<ValidationResult>();
        DateGenerationWorker worker = new DateGenerationWorker(resultQueue, "worker-1", user, pass, url);
        service.submit(worker);
        worker = new DateGenerationWorker(resultQueue, "worker-2", user, pass, url);
        service.submit(worker);
        worker = new DateGenerationWorker(resultQueue, "worker-3", user, pass, url);
        service.submit(worker);
        worker = new DateGenerationWorker(resultQueue, "worker-4", user, pass, url);
        service.submit(worker);
        worker = new DateGenerationWorker(resultQueue, "worker-5", user, pass, url);
        service.submit(worker);
        int counter = 1;
        do {
            final ValidationResult validationResult = resultQueue.take();
            if ( !validationResult.isSuccess() ) {
                System.out.println(validationResult);
                throw new IllegalStateException();
            } else {
                System.out.println("successful: " + validationResult);
            }
        } while ( counter < 10000 );

        System.out.println("Safely Quit.");
    }
}
