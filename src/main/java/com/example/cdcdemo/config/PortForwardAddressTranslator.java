package com.example.cdcdemo.config;

import com.datastax.oss.driver.api.core.addresstranslation.AddressTranslator;
import com.datastax.oss.driver.api.core.context.DriverContext;

import java.net.InetSocketAddress;
import java.util.Map;


public class PortForwardAddressTranslator implements AddressTranslator {

    private static final Map<String, InetSocketAddress> TRANSLATIONS = Map.of(
            "10.100.17.48", new InetSocketAddress("127.0.0.2", 9042),
            "10.100.21.241", new InetSocketAddress("127.0.0.3", 9042),
            "10.100.23.155", new InetSocketAddress("127.0.0.4", 9042)
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
