package com.xuwuji.twitter.storm.trident.operation;

import java.util.Map;

import com.google.code.geocoder.Geocoder;
import com.google.code.geocoder.GeocoderRequestBuilder;
import com.google.code.geocoder.model.GeocodeResponse;
import com.google.code.geocoder.model.GeocoderRequest;
import com.google.code.geocoder.model.GeocoderResult;
import com.google.code.geocoder.model.GeocoderStatus;
import com.google.code.geocoder.model.LatLng;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/**
 * get the lat and long based on the location
 * 
 * @author wuxu 2016-3-31
 *
 */
public class GeoParser extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private Geocoder geocoder;

	public void prepare(Map conf, TridentOperationContext context) {
		geocoder = new Geocoder();
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Values values = new Values();
		String location = tuple.getString(0);
		GeocoderRequest request = new GeocoderRequestBuilder().setAddress(location).setLanguage("en")
				.getGeocoderRequest();
		GeocodeResponse response = geocoder.geocode(request);
		GeocoderStatus status = response.getStatus();
		if (GeocoderStatus.OK.equals(status)) {
			GeocoderResult firstResult = response.getResults().get(0);
			LatLng latLng = firstResult.getGeometry().getLocation();
			System.out.println(latLng.toString());
			// System.out.println("lat: " + latLng.getLat());
			// System.out.println("lng: " + latLng.getLng());
			values.add(latLng.getLat());
			values.add(latLng.getLng());
			collector.emit(values);
		}

	}

}
