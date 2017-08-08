package json2csv;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;

public class ElasticSearchUriBuilder {
	public ElasticSearchUriBuilder(){
		
	}
	
	public URI getURI(String url) throws URISyntaxException, MalformedURLException{
		URL url_ = new URL(url);
		return new URI(url_.getProtocol(), url_.getHost() + ":" + url_.getPort(), url_.getPath(), url_.getQuery(), null);
	}
	
	public URI getURI(String url, Integer from, Integer size) throws URISyntaxException, MalformedURLException{		
		url = setRangeParameters(url, from, size);
		return getURI(url);
	}

	public String setRangeParameters(String url, Integer from, Integer size){
		url = putQueryParameterValue(url, "from", String.valueOf(from));
		url = putQueryParameterValue(url, "size", String.valueOf(size));
		
		return url;
	}
	
	private String putQueryParameterValue(String url, String parameterName, String value){
		String newQuery, suffix;
		int parameterIndex = url.indexOf(parameterName);
		
		if(parameterIndex < 0){
			newQuery = addQueryParameterAndValue(url, parameterName, value);
		}
		else{
			newQuery = url.substring(0, url.indexOf(":", parameterIndex)+1);
			newQuery += value;
			suffix = url.substring(url.indexOf(":", parameterIndex)+1);
			suffix = suffix.replaceFirst("[0-9]+", "");
			newQuery += suffix;
		}
		
		return newQuery;
	}
	
	private String addQueryParameterAndValue(String url, String parameterName, String value){
		String newUrl = url.substring(0, url.lastIndexOf("}"));
		newUrl += ",\"" + parameterName + "\":" + value + url.substring(url.lastIndexOf("}"));
		
		return newUrl;
	}
}
