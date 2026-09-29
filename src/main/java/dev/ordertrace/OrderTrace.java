package dev.ordertrace;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(Settings.class)
public class OrderTrace {
    public static void main(String[] args) throws Exception {
        if (args.length > 0 && !args[0].startsWith("--")) { Commands.run(args); return; }
        SpringApplication.run(OrderTrace.class, args);
    }
}
