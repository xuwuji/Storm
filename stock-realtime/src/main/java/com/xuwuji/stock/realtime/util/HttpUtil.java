package com.xuwuji.stock.realtime.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLConnection;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.NameValuePair;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.commons.httpclient.params.HttpClientParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.xuwuji.stock.realtime.constants.Constants;

/**
 * Http Util
 * 
 * get method
 * 
 * post method -application/json
 * 
 * @author xuwuji
 *
 */
public class HttpUtil {

	private static final Logger LOG = LoggerFactory.getLogger(HttpUtil.class);

	public static String getHttpResponse(HttpMethod request) throws Exception {
		HttpClient client = new HttpClient();

		String response = "";
		int status = -1;
		try {
			LOG.info(request.getURI().toString());
			status = client.executeMethod(request);
			if (status != 200) {
				throw new RuntimeException("Got unexpected response code " + status);
			}
			response = request.getResponseBodyAsString();
		} catch (HttpException e) {
			LOG.error("Fatal protocol violation: " + e.getMessage());
			throw e;
		} catch (IOException e) {
			LOG.error("Fatal transport error: " + e.getMessage());
			throw e;
		} finally {
			// Release the connection.
			request.releaseConnection();
		}
		return response;
	}

	/**
	 * get the response from a get method
	 * 
	 * @param url
	 * @return
	 * @throws Exception
	 */
	public static String Get(String url) throws Exception {
		GetMethod request = null;
		request = new GetMethod(url);
		return getHttpResponse(request);
	}

	public static URLConnection setRequestHeader(URLConnection conn) {
		for (Entry<String, String> entry : Constants.HEADER.entrySet()) {
			conn.setRequestProperty(entry.getKey(), entry.getValue());
		}
		return conn;
	}

	/**
	 * get the response from a post method, based on json
	 * 
	 * @param url
	 * @param payload
	 * @return
	 * @throws Exception
	 */
	public static String postJson(String url, String payload) throws Exception {
		StringRequestEntity requestEntity = new StringRequestEntity(payload, "application/json", "UTF-8");
		PostMethod request = new PostMethod(url);
		request.setRequestEntity(requestEntity);
		String response = HttpUtil.getHttpResponse(request);
		System.out.println(response);
		return response;
	}

	/**
	 * encode the param
	 * 
	 * @param param
	 * @return
	 * @throws UnsupportedEncodingException
	 */
	public static String encode(String param, String charset) throws UnsupportedEncodingException {
		param = java.net.URLEncoder.encode(param, charset);
		return param;
	}

	public static String outputResponse(URLConnection conn) throws IOException {
		BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
		String result = "";
		String line;
		while ((line = in.readLine()) != null) {
			result += line;
		}
		return result;
	}
}