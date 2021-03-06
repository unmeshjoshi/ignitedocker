package com.demobank.cache.ignite;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.io.Serializable;
import java.util.Arrays;

public class IgniteFactory implements Serializable {

    public Ignite startOrGetIgniteInClientMode() {
        // Start Ignite in client mode.
        Ignition.setClientMode(true);
        IgniteConfiguration cfg = new IgniteConfiguration();
        cfg.setPeerClassLoadingEnabled(true);
        cfg.setWorkDirectory("/tmp/work");
        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();
        TcpDiscoveryVmIpFinder ipFinder = new TcpDiscoveryVmIpFinder();
        ipFinder.setAddresses(Arrays.asList("localhost:47500"));
        discoSpi.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(discoSpi);
        Ignite ignite = Ignition.getOrStart(cfg);
        return ignite;
    }
}
