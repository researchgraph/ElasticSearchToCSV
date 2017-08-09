package json2csv;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Properties;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

public class ElasticSearchRequest {
	private CloseableHttpClient httpClient;
	
	public ElasticSearchRequest(){
		httpClient = HttpClients.createDefault();
	}
	
	
	public String getJSONString(String url, Integer from, Integer size) throws URISyntaxException, MalformedURLException{
		return httpResponseAsText(httpGetRequest((new ElasticSearchUriBuilder()).getURI(url, from, size)));
	}
	
	
	public int getTotalHitCountFromJson(String url) throws URISyntaxException, MalformedURLException{
		String hitCountParameterString = "\"hits\":{\"total\":";
		String hitCount;
				
		String response = this.getJSONString(url, 0, 1);
		if(response == null || response.isEmpty())
			return 0;
		
		if(!response.contains(hitCountParameterString))
			return 0;
		
		hitCount = response.substring(response.indexOf(hitCountParameterString));
		hitCount = hitCount.substring(0, hitCount.indexOf(","));
		hitCount = hitCount.substring(hitCount.lastIndexOf(":") +1);
				
		return Integer.valueOf(hitCount.trim());
	}
	
	public int getTotalHitCount(Properties properties){
		return getIntegerParamterValue("size", properties.getProperty("parameters.source"));
	}
	
	public int getStartValue(Properties properties){
		return getIntegerParamterValue("from", properties.getProperty("parameters.source"));
	}
	
	public int getTotalHitCount(String url){
		return getIntegerParamterValue("size", url);
	}
	
	public int getStartValue(String url){
		return getIntegerParamterValue("from", url);
	}
	
	public int getIntegerParamterValue(String parameterName, String text){
		String parameterValue = getParameterValue(parameterName, text);
		if(parameterValue.isEmpty())
			return -1;
		return Integer.valueOf(parameterValue);
	}
	
	public String getParameterValue(String parameterName, String text){
		String value = "";
		
		if(text.contains(parameterName)){
			value = text.substring(text.indexOf(parameterName));
			value = value.substring(value.indexOf(":")+1, value.indexOf(","));
		}
		
		return value.trim();
	}
	
	
//	##### http communication #####
	
	public HttpResponse httpGetRequest(URI searchUri){
		System.out.println(searchUri.toString());
		HttpResponse response = null;
		try {
			HttpGet request = new HttpGet(searchUri);
			response = httpClient.execute(request);
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return response;
	}
		
	
	public String httpResponseAsText(HttpResponse response){
		String responseAsText = "";
		try {
			InputStreamReader isr = new InputStreamReader(response.getEntity().getContent(), "UTF-8");
			BufferedReader br = new BufferedReader(isr);
			String line;
			while((line = br.readLine()) != null){
				responseAsText += line + "\n";
			}
		} catch (IllegalStateException | IOException e) {
			e.printStackTrace();
		}
		return responseAsText;
	}
}
