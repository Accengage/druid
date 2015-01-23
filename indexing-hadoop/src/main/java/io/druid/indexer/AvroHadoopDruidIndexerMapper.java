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

package io.druid.indexer;

import com.metamx.common.RE;
import com.metamx.common.logger.Logger;

import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.segment.indexing.granularity.GranularitySpec;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.joda.time.DateTime;

import java.io.IOException;

public abstract class AvroHadoopDruidIndexerMapper<KEYOUT, VALUEOUT> extends Mapper<LongWritable, AvroValue<GenericRecord>, KEYOUT, VALUEOUT>
{
  private HadoopDruidIndexerConfig config;
  private StringInputRowParser parser;
  private static final Logger log = new Logger(AvroHadoopDruidIndexerMapper.class);

  @Override
  protected void setup(Context context)
      throws IOException, InterruptedException
  {
    config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
    parser = config.getParser();
  }

  public HadoopDruidIndexerConfig getConfig()
  {
    return config;
  }

  public StringInputRowParser getParser()
  {
    return parser;
  }

  @Override
  protected void map(
      LongWritable key, AvroValue<GenericRecord> value, Context context
  ) throws IOException, InterruptedException
  {
    try {
      final InputRow inputRow;
      try {
		  GenericRecord grValue = value.datum();
	  	  log.info("Generic Record " + grValue.toString());
	      inputRow = parser.parse(grValue.toString());
      }
      catch (Exception e) {
        if (config.isIgnoreInvalidRows()) {
          context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
          return; // we're ignoring this invalid row
        } else {
          throw e;
        }
      }
      GranularitySpec spec = config.getGranularitySpec();
      if (!spec.bucketIntervals().isPresent() || spec.bucketInterval(new DateTime(inputRow.getTimestampFromEpoch()))
                                                     .isPresent()) {
        innerMap(inputRow, value, context);
      }
    }
    catch (RuntimeException e) {
      throw new RE(e, "Failure on row[%s]", value);
    }
  }

  abstract protected void innerMap(InputRow inputRow, AvroValue<GenericRecord> record, Context context)
      throws IOException, InterruptedException;
}