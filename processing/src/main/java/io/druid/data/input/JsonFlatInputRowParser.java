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
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ToLowerCaseParser;

import io.druid.data.input.impl.DataSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.SpatialDimensionSchema;
import io.druid.data.input.impl.TimestampSpec;

import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonTypeName("jsonFlat")
public class JsonFlatInputRowParser implements ByteBufferInputRowParser
{
	  private static final Logger log = new Logger(StringInputRowParser.class);

	  private final ParseSpec parseSpec;
	  private final MapInputRowParser mapParser;
	  private final Parser<String, Object> parser;

	  private CharBuffer chars = null;
	  private final String separator = ">";

	  @JsonCreator
	  public JsonFlatInputRowParser(
	      @JsonProperty("parseSpec") ParseSpec parseSpec,
	      // Backwards compatible
	      @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
	      @JsonProperty("data") final DataSpec dataSpec,
	      @JsonProperty("dimensions") List<String> dimensions,
	      @JsonProperty("dimensionExclusions") List<String> dimensionExclusions
	  )
	  {
	    if (parseSpec == null) {
	      if (dataSpec == null) {
	        this.parseSpec = new JSONParseSpec(
	            timestampSpec,
	            new DimensionsSpec(
	                dimensions,
	                dimensionExclusions,
	                ImmutableList.<SpatialDimensionSchema>of()
	            )
	        );
	      } else {
	        this.parseSpec = dataSpec.toParseSpec(timestampSpec, dimensionExclusions);
	      }
	      this.mapParser = new MapInputRowParser(this.parseSpec, null, null, null, null);
	      this.parser = new ToLowerCaseParser(this.parseSpec.makeParser());
	    } else {
	      this.parseSpec = parseSpec;
	      this.mapParser = new MapInputRowParser(parseSpec, null, null, null, null);
	      this.parser = new ToLowerCaseParser(parseSpec.makeParser());
	    }
	  }

	  @Override
	  public InputRow parse(ByteBuffer input)
	  {
	    return parseMap(flatten(buildStringKeyMap(input)));
	  }

	  @JsonProperty
	  @Override
	  public ParseSpec getParseSpec()
	  {
	    return parseSpec;
	  }

	  @Override
	  public StringInputRowParser withParseSpec(ParseSpec parseSpec)
	  {
	    return new StringInputRowParser(parseSpec, null, null, null, null);
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
		  //test if it's a string leaf or a node
		  if (nestedObject instanceof Map<?,?>){
			Map<String, Object> node = (Map<String, Object>) nestedObject;
			Map<String, Object> flatObject = new HashMap<String, Object>();
			for (Map.Entry<String, Object> entry : node.entrySet())
			{
				if (entry.getValue() instanceof Map<?,?>){
					Map<String, Object> flatLeaf = flatten(entry.getValue());
					for (Map.Entry<String, Object> entryLeaf : node.entrySet())
					{
						flatObject.put(entry.getKey() + separator + entryLeaf.getKey(), entryLeaf.getValue());
					}
				}
				else{
					flatObject.put(entry.getKey(), entry.getValue());
				}
			}
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
