package com.sliit.api_gateway;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;

@WebFluxTest
class ApiGatewayApplicationTests {

    @Test
    void contextLoads() {
        // Will load only WebFlux components, not the full Gateway
    }
}