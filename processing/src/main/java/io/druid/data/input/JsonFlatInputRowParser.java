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
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonTypeName("jsonFlat")
public class JsonFlatInputRowParser implements ByteBufferInputRowParser
{
  private static final Logger log = new Logger(JsonFlatInputRowParser.class);

  private final ParseSpec parseSpec;
  private final MapInputRowParser mapParser;
  private final Parser<String, Object> parser;
  private final String separator;

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
	  return parseMap(flatten(buildStringKeyMap(input)));
  }

  private Map<String, Object> buildStringKeyMap(ByteBuffer input)
  {
	//Using Jackson's parser to get a fully nested object.
	Map<String, Object> theMap;
	ObjectMapper mapper = new ObjectMapper();
	try {
		theMap = mapper.readValue(input.array(), HashMap.class);
		
	} catch (IOException e) {
		throw new ParseException("Failed with exception caused by ", e.getCause());
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
					flatObject.put(entry.getKey().toLowerCase() + separator + entryLeaf.getKey().toLowerCase(), entryLeaf.getValue());
				}
			}
			else{
				flatObject.put(entry.getKey().toLowerCase(), entry.getValue());
			}
		}
		 log.warn("Return: " + flatObject.toString());
		return flatObject;
	  }
	  //Impossible Case: if object given is not a map it's not a valid json message
	  throw new ParseException("Invalid Json");
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
