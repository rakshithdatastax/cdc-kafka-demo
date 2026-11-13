package com.example.cdcdemo.config;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;

import java.net.InetSocketAddress;
import java.util.Map;


public class PortForwardAddressTranslator implements AddressTranslator {

    // Pod IPs are ephemeral -- update these whenever the dev-cassandra-dc1-rack1-sts-{0,1,2}
    // pods get recreated (e.g. after a CassandraDatacenter resource-spec change or any rolling
    // restart). Current mapping as of 2026-10-22.
    private static final Map<String, InetSocketAddress> TRANSLATIONS = Map.of(
            "10.100.16.107", new InetSocketAddress("127.0.0.2", 9042), // dev-cassandra-dc1-rack1-sts-0
            "10.100.21.112", new InetSocketAddress("127.0.0.3", 9042), // dev-cassandra-dc1-rack1-sts-1
            "10.100.21.194", new InetSocketAddress("127.0.0.4", 9042)  // dev-cassandra-dc1-rack1-sts-2
    );

    public PortForwardAddressTranslator(DriverContext context) {
    }

    @Override
    public InetSocketAddress translate(InetSocketAddress address) {
        InetSocketAddress translated = TRANSLATIONS.get(address.getHostString());
        return translated != null ? translated : address;
    }

    @Override
    public void close() {
    }
}
