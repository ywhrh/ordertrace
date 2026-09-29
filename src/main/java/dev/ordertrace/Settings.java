package dev.ordertrace;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ordertrace")
public record Settings(String schema, String bootstrap, String applicationId, String rawTopic,
                       String resultTopic, String stateDir, boolean pipelineEnabled) {
    public Settings {
        if (schema == null || !schema.matches("[a-z][a-z0-9_]{0,62}")) throw new IllegalArgumentException("Invalid schema");
        if (applicationId == null || resultTopic == null || applicationId.isBlank() || resultTopic.isBlank())
            throw new IllegalArgumentException("Application ID and result topic required");
    }
    public String writerGroup() { return applicationId + "-db-" + schema; }
}
