package com.example.cdcdemo.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.auth.ProgrammaticPlainTextAuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(DemoProperties.class)
public class CassandraConfig {

    @Bean(destroyMethod = "close")
    public CqlSession cqlSession(DemoProperties props) {
        DemoProperties.Cassandra c = props.getCassandra();
        DriverConfigLoader configLoader = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(15))
                .build();
        return CqlSession.builder()
                .addContactPoint(new InetSocketAddress(c.getContactPoint(), c.getPort()))
                .withLocalDatacenter(c.getLocalDatacenter())
                .withAuthProvider(new ProgrammaticPlainTextAuthProvider(c.getUsername(), c.getPassword()))
                .withConfigLoader(configLoader)
                .build();
    }
}
