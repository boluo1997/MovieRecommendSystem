package boluo;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class ES_Test {

	private String IP;
	private int PORT;

	@Before
	public void init() {
		this.IP = "192.168.177.128";
		this.PORT = 9300;
	}

//	@Test
//	public void esClient() {
//		try {
//			Settings settings = Settings.builder().put("cluster.name", "my-application").build();
//			TransportClient client = new PreBuiltTransportClient(settings)
//					.addTransportAddresses(new TransportAddress(InetAddress.getByName(IP), PORT));
//
//			System.out.println("连接成功: " + client.toString());
//		} catch (UnknownHostException e) {
//			e.printStackTrace();
//		}
//	}

}
