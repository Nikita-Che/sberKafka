package org.example;

import org.example.lesson1.service.ProducerService;

public class App 
{
    public static void main( String[] args )    {
        ProducerService producerService = new ProducerService();
        producerService.send();

    }
}
