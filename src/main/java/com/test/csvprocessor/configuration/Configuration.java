package com.test.csvprocessor.configuration;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;


@Getter
@Setter
@ConfigurationProperties(prefix = "application")
public class Configuration {

    private String inputDir;
    private String outputDir;
    private String JDBC_DRIVER;
    private String sysDB;
    private String url;
    private String user;
    private String password;
}