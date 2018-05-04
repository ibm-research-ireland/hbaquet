package org.apache.hadoop.hbase.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class HBaquetGroupWriteSupport extends WriteSupport<Group> {

    private GroupWriter groupWriter = null;
    private MessageType schema;

    public HBaquetGroupWriteSupport(MessageType schema) {
        this.schema = schema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        return new WriteContext(schema, new java.util.HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        groupWriter = new GroupWriter(recordConsumer, schema);
    }

    @Override
    public void write(Group record) {
        groupWriter.write(record);
    }
}
