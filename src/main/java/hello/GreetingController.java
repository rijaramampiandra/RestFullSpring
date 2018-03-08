package hello;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class GreetingController {

	private static final String template = "Hello, %s!";
	private final AtomicLong counter = new AtomicLong();

	@Value("${elasticsearch.index}")
	private String esIndex;

	@Value("${elasticsearch.index.id}")
	private Integer esId;

	@Value("${elasticsearch.host}")
	private String esHost;

	@Value("${elasticsearch.url}")
	private String esUrl;

	@Value("${elasticsearch.port}")
	private Integer esPort;

	@CrossOrigin(origins = "http://localhost:4200")
	@RequestMapping("/greeting")
	public Greeting greeting(@RequestParam(value = "name", defaultValue = "World") String name) throws IOException {

		RestClient restClient = RestClient
				.builder(new HttpHost(esUrl, esPort, esHost)).build();

		searchData(restClient);

		Map<String, String> params = Collections.emptyMap();
		String jsonString = "{" + "\"title\":\"Spring + Spring Data + ElasticSearch\","
				+ "\"category\":\"Spring Boot\"," + "\"published_date\":\"JAVA-08-MAR-2017\","
				+ "\"author\":\"Rija RAMAMPIANDRA\"" + "}";
		HttpEntity entity = new NStringEntity(jsonString, ContentType.APPLICATION_JSON);
		Response response = restClient.performRequest("PUT", esIndex + "/" + esId , params, entity);
		if (response.getEntity() != null) {
			System.out.println("Entity not null");
		}
		return new Greeting(counter.incrementAndGet(), String.format(template, name));
	}

	private void searchData(RestClient restClient) throws IOException {
		Map<String, String> params = Collections.emptyMap();
		HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory consumerFactory = new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(
				30 * 1024 * 1024);
		Response response = restClient.performRequest("GET", esIndex + "/_search", params, null, consumerFactory);
		if (response.getEntity() != null && response.getStatusLine() != null) {
			System.out.println("Entity not null" + response.getStatusLine().getStatusCode());
		}

	}
}
