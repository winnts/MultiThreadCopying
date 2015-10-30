import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

/**
 * Created by adyachenko on 26.10.15.
 */
public class ConnectAsTransport {
    public static Client connectToEs (String clusterName, String clusterIPAddress, Integer clusterPort) {
        //Start of Default settings
        String defaultClusterName = "test-cluster";
        String defaultClusterIP = "localhost";
   //     Integer defaultClusterPort = 9300;
        //End of Default Settings

        if (clusterName.equals("")) {clusterName = defaultClusterName;}
        if (clusterIPAddress.equals("")) {clusterIPAddress = defaultClusterIP;}
        Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", clusterName).put("client.transport.ping_timeout", "120s").build();
        Client client = new TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(clusterIPAddress, clusterPort)) ;

        return client;
    }
}
