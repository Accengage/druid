package com.accengage.mapreduce.avro;
import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * Reads records from an input split representing a chunk of an Avro container file.
 *
 * @param <T> The (java) type of data in Avro container file.
 */
public class AvroPositionRecordReader<T> extends AvroPositionRecordReaderBase<LongWritable, AvroValue<T>, T> {
  private static final Logger LOG = LoggerFactory.getLogger(AvroPositionRecordReader.class);
  /** A reusable object to hold records of the Avro container file. */
  private final AvroValue<T> mCurrentRecord;
  private final LongWritable mStartPosition;
  /**
   * Constructor.
   *
   * @param readerSchema The reader schema to use for the records in the Avro container file.
   */
  public AvroPositionRecordReader(Schema readerSchema) {
    super(readerSchema);
    mCurrentRecord = new AvroValue<T>(null);
    mStartPosition = new LongWritable(0L) ;
  }
  /** {@inheritDoc} */
  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    boolean hasNext = super.nextKeyValue();
    mCurrentRecord.datum(getCurrentRecord());
    mStartPosition.set(getStartPosition());
    return hasNext;
  }
  /** {@inheritDoc} */
  @Override
  public LongWritable getCurrentKey() throws IOException, InterruptedException {
    return mStartPosition;
  }
  /** {@inheritDoc} */
  @Override
  public AvroValue<T> getCurrentValue() throws IOException, InterruptedException {
    return mCurrentRecord;
  }
}
