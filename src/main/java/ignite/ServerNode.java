package ignite;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.BinaryConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

import java.util.Arrays;

public class ServerNode {
    public static void main(String[] args) {
        Ignition.start(new IgniteConfiguration()
                .setDeploymentMode(DeploymentMode.PRIVATE)
                .setPeerClassLoadingEnabled(true)
                .setBinaryConfiguration(new BinaryConfiguration()
                        .setCompactFooter(true))
                .setCommunicationSpi(new TcpCommunicationSpi()
                        .setLocalAddress("localhost"))
                .setTransactionConfiguration(new TransactionConfiguration()
                        .setDefaultTxTimeout(5_000L))
                .setDiscoverySpi(new TcpDiscoverySpi()
                        .setIpFinder(new TcpDiscoveryVmIpFinder()
                                .setAddresses(Arrays.asList("127.0.0.1:47500..47509")
                                )
                        )
                )

        );
    }
}