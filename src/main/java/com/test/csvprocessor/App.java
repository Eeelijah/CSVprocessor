package com.test.csvprocessor;

import com.test.csvprocessor.configuration.Configuration;
import com.test.csvprocessor.services.UploaderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;


@SpringBootApplication
@EnableConfigurationProperties(Configuration.class)
public class App implements CommandLineRunner {

    @Autowired
    private UploaderService uploaderService;

    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }

    @Override
    public void run(String... strings) throws Exception {
        uploaderService.processNewFiles("admin", "admin", "test");
    }

}
