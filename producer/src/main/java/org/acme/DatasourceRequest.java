package org.acme;

import lombok.Builder;
import lombok.extern.jackson.Jacksonized;

@Jacksonized
@Builder
public record DatasourceRequest(String redisHost, Integer redisPort, String message, String uniqueIdentifier) {
}
