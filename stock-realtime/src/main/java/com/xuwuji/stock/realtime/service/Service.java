package com.xuwuji.stock.realtime.service;

import java.io.IOException;

import org.json.simple.parser.ParseException;

public interface Service {

	public Object run() throws IOException, ParseException;

}
