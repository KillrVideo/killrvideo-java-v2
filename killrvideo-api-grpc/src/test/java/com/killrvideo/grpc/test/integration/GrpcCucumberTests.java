package com.killrvideo.grpc.test.integration;

import org.junit.Ignore;
import org.junit.runner.RunWith;

import cucumber.api.CucumberOptions;
import cucumber.api.junit.Cucumber;

@RunWith(Cucumber.class)
@CucumberOptions(
        strict=false,
        plugin = {"progress", "html:/tmp/cucumber-report"},
        features = "src/test/resources")
@Ignore
public class GrpcCucumberTests {
   
    
}
