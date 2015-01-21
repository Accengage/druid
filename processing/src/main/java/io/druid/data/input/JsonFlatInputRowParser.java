/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.data.input;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.TimestampSpec;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.FileDescriptor;

@JsonTypeName("jsonFlat")
public class JsonFlatInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger log = new Logger(JsonFlatInputRowParser.class);

  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Parser<String, Object> parser;
  private final String separator;
  private CharBuffer chars = null;

  @JsonCreator
  public JsonFlatInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("separator") String separator,
      // Backwards compatible
      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      @JsonProperty("dimensions") List<String> dimensions,
      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions,
      @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions
  )
  {
	  // Backwards Compatible
    if (parseSpec == null) {
      this.parseSpec = new JSONParseSpec(
          timestampSpec,
          new DimensionsSpec(dimensions, dimensionExclusions, spatialDimensions)
      );
    } else {
      this.parseSpec = parseSpec;
    }
    
    if(separator == null){
    	this.separator = ">";
    }
    else{
    	this.separator = separator;
    }
    this.mapParser = new MapInputRowParser(this.parseSpec, null, null, null, null);
    this.parser = new ToLowerCaseParser(parseSpec.makeParser());
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public JsonFlatInputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new JsonFlatInputRowParser(parseSpec, separator, null, null, null, null);
  }

  @Override
  public InputRow parse(ByteBuffer input)
  {
	  Map<String,String> map = new HashMap<String,String>();
	  
	  ObjectMapper mapper = new ObjectMapper();
	  try {
		map = mapper.readValue(input.array(), HashMap.class);
		return parseMap(flatten(map));
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	  return null;
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
    int payloadSize = input.remaining();

    if (chars == null || chars.remaining() < payloadSize) {
      chars = CharBuffer.allocate(payloadSize);
    }

    final CoderResult coderResult = Charsets.UTF_8.newDecoder()
                                                  .onMalformedInput(CodingErrorAction.REPLACE)
                                                  .onUnmappableCharacter(CodingErrorAction.REPLACE)
                                                  .decode(input, chars, true);

    Map<String, Object> theMap;
    if (coderResult.isUnderflow()) {
      chars.flip();
      try {
        theMap = parseString(chars.toString());
      }
      finally {
        chars.clear();
      }
    } else {
      throw new ParseException("Failed with CoderResult[%s]", coderResult);
    }
    return theMap;
  }
  
  private Map<String, Object> flatten(Object nestedObject){
	  log.warn("Flattening: " + nestedObject.toString());
	  //test if it's a string leaf or a node
	  if (nestedObject instanceof Map<?,?>){
		Map<String, Object> node = (Map<String, Object>) nestedObject;
		Map<String, Object> flatObject = new HashMap<String, Object>();
		for (Map.Entry<String, Object> entry : node.entrySet())
		{
			if (entry.getValue() instanceof Map<?,?>){
				Map<String, Object> flatLeaf = flatten(entry.getValue());
				for (Map.Entry<String, Object> entryLeaf : flatLeaf.entrySet())
				{
					flatObject.put(entry.getKey() + separator + entryLeaf.getKey(), entryLeaf.getValue());
				}
			}
			else{
				flatObject.put(entry.getKey(), entry.getValue());
			}
		}
		 log.warn("Return: " + flatObject.toString());
		return flatObject;
	  }
	  //Impossible: if object given is not a map it's not a valid json message
	  return null;
  }
  
  private Map<String, Object> parseString(String inputString)
  {
    return parser.parse(inputString);
  }

  public InputRow parse(String input)
  {
    return parseMap(parseString(input));
  }

  private InputRow parseMap(Map<String, Object> theMap)
  {
    return mapParser.parse(theMap);
  }
}


///*
// * Druid - a distributed column store.
// * Copyright (C) 2012, 2013  Metamarkets Group Inc.
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
// */
//
//package io.druid.data.input;
//
//import com.fasterxml.jackson.annotation.JsonCreator;
//import com.fasterxml.jackson.annotation.JsonProperty;
//import com.fasterxml.jackson.annotation.JsonTypeName;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import com.google.common.base.Charsets;
//import com.google.common.base.Throwables;
//import com.google.common.collect.ImmutableList;
//import com.google.common.collect.Maps;
//import com.metamx.common.logger.Logger;
//import com.metamx.common.parsers.ParseException;
//import com.metamx.common.parsers.Parser;
//import com.metamx.common.parsers.ToLowerCaseParser;
//
//import io.druid.data.input.impl.DataSpec;
//import io.druid.data.input.impl.DimensionsSpec;
//import io.druid.data.input.impl.JSONParseSpec;
//import io.druid.data.input.impl.MapInputRowParser;
//import io.druid.data.input.impl.StringInputRowParser;
//import io.druid.data.input.impl.ParseSpec;
//import io.druid.data.input.impl.SpatialDimensionSchema;
//import io.druid.data.input.impl.TimestampSpec;
//
//import java.io.InputStream;
//import java.nio.ByteBuffer;
//import java.nio.CharBuffer;
//import java.nio.charset.CoderResult;
//import java.nio.charset.CodingErrorAction;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//@JsonTypeName("jsonFlat")
//public class JsonFlatInputRowParser implements ByteBufferInputRowParser
//{
//	  private static final Logger log = new Logger(JsonFlatInputRowParser.class);
//
//	  private final ParseSpec parseSpec;
//	  private final MapInputRowParser mapParser;
//	  private final Parser<String, Object> parser;
//
//	  private CharBuffer chars = null;
//	  private final String separator = ">";
//
//	  @JsonCreator
//	  public JsonFlatInputRowParser(
//		      @JsonProperty("parseSpec") ParseSpec parseSpec,
//		      @JsonProperty("descriptor") String descriptorFileInClasspath,
//		      // Backwards compatible
//		      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
//		      @JsonProperty("dimensions") List<String> dimensions,
//		      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions,
//		      @JsonProperty("spatialDimensions") List<SpatialDimensionSchema> spatialDimensions
//	  )
//	  {
//		  log.warn("Creating JsonFlatInputRowParser");
//		// Backwards Compatible
//		    if (parseSpec == null) {
//		    	log.warn("parseSpec is null");
//		      this.parseSpec = new JSONParseSpec(
//		          timestampSpec,
//		          new DimensionsSpec(dimensions, dimensionExclusions, spatialDimensions)
//		      );
//		    } else {
//		      this.parseSpec = parseSpec;
//		    }
//		    this.mapParser = new MapInputRowParser(this.parseSpec, null, null, null, null);
//		    this.parser = new ToLowerCaseParser(parseSpec.makeParser());
//		  
//		
////	    if (parseSpec == null) {
////	    	log.warn("parseSpec is null");
////	      if (dataSpec == null) {
////	    	log.warn("dataSpec is null");
////	        this.parseSpec = new JSONParseSpec(
////	            timestampSpec,
////	            new DimensionsSpec(
////	                dimensions,
////	                dimensionExclusions,
////	                ImmutableList.<SpatialDimensionSchema>of()
////	            )
////	        );
////	      } else {
////	    	  log.warn("Deprecated Stuff");
////	        this.parseSpec = dataSpec.toParseSpec(timestampSpec, dimensionExclusions);
////	      }
////	      this.mapParser = new MapInputRowParser(this.parseSpec, null, null, null, null);
////	      this.parser = new ToLowerCaseParser(this.parseSpec.makeParser());
////	    } else {
////	      this.parseSpec = parseSpec;
////	      this.mapParser = new MapInputRowParser(parseSpec, null, null, null, null);
////	      this.parser = new ToLowerCaseParser(parseSpec.makeParser());
////	    }
//	    log.warn("JsonFlatInputRowParser Created");
//	  }
//
//	  @Override
//	  public InputRow parse(ByteBuffer input)
//	  {
//	    return parseMap(flatten(buildStringKeyMap(input)));
//	  }
//
//	  @JsonProperty
//	  @Override
//	  public ParseSpec getParseSpec()
//	  {
//	    return parseSpec;
//	  }
//
//	  @Override
//	  public StringInputRowParser withParseSpec(ParseSpec parseSpec)
//	  {
//	    return new StringInputRowParser(parseSpec, null, null, null, null);
//	  }
//
//	  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
//	  {
//	    int payloadSize = input.remaining();
//
//	    if (chars == null || chars.remaining() < payloadSize) {
//	      chars = CharBuffer.allocate(payloadSize);
//	    }
//
//	    final CoderResult coderResult = Charsets.UTF_8.newDecoder()
//	                                                  .onMalformedInput(CodingErrorAction.REPLACE)
//	                                                  .onUnmappableCharacter(CodingErrorAction.REPLACE)
//	                                                  .decode(input, chars, true);
//
//	    Map<String, Object> theMap;
//	    if (coderResult.isUnderflow()) {
//	      chars.flip();
//	      try {
//	        theMap = parseString(chars.toString());
//	      }
//	      finally {
//	        chars.clear();
//	      }
//	    } else {
//	      throw new ParseException("Failed with CoderResult[%s]", coderResult);
//	    }
//	    return theMap;
//	  }
//	  
//	  private Map<String, Object> flatten(Object nestedObject){
//		  //test if it's a string leaf or a node
//		  if (nestedObject instanceof Map<?,?>){
//			Map<String, Object> node = (Map<String, Object>) nestedObject;
//			Map<String, Object> flatObject = new HashMap<String, Object>();
//			for (Map.Entry<String, Object> entry : node.entrySet())
//			{
//				if (entry.getValue() instanceof Map<?,?>){
//					Map<String, Object> flatLeaf = flatten(entry.getValue());
//					for (Map.Entry<String, Object> entryLeaf : node.entrySet())
//					{
//						flatObject.put(entry.getKey() + separator + entryLeaf.getKey(), entryLeaf.getValue());
//					}
//				}
//				else{
//					flatObject.put(entry.getKey(), entry.getValue());
//				}
//			}
//			return flatObject;
//		  }
//		  //Impossible: if object given is not a map it's not a valid json message
//		  return null;
//	  }
//
//	  private Map<String, Object> parseString(String inputString)
//	  {
//	    return parser.parse(inputString);
//	  }
//
//	  public InputRow parse(String input)
//	  {
//	    return parseMap(parseString(input));
//	  }
//
//	  private InputRow parseMap(Map<String, Object> theMap)
//	  {
//	    return mapParser.parse(theMap);
//	  }
//}
