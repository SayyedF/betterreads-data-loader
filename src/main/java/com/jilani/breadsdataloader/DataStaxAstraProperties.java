package com.jilani.breadsdataloader;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.io.File;

@ConfigurationProperties(prefix = "datastax.astra")
public class DataStaxAstraProperties {

    private File secureConnectBundle;

    public File getSecureConnectBundle() {
        return secureConnectBundle;
    }

    public void setSecureConnectBundle(File secureConnectBundle) {
        this.secureConnectBundle = secureConnectBundle;
    }
}