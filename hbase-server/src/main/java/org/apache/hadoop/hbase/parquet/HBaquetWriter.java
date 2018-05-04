package org.apache.hadoop.hbase.parquet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparatorImpl;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.io.hfile.HFileWriterImpl;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;
import org.apache.yetus.audience.InterfaceAudience;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Private
public class HBaquetWriter {

    private static final Log LOG = LogFactory.getLog(HFileWriterImpl.class);

    protected final Path path;
    protected final Configuration conf;
    protected final CellComparatorImpl comparator;
    private ParquetWriter<Group> writer;
    private Cell lastCell;
    private HashMap<String,Cell> bufferForRow;
    private MessageType schema;

    public HBaquetWriter(final Configuration conf, Path path, String parquetSchema) {
        this.conf = conf;
        this.path = path;
        this.comparator = new CellComparatorImpl();
        schema = MessageTypeParser.parseMessageType("message schema "+parquetSchema);
        WriteSupport<Group> writeSupport = new HBaquetGroupWriteSupport(schema);
        try {
            LOG.info("initialize writer on " + this.path);
            writer = new ParquetWriter<Group>(this.path, writeSupport);
        } catch (Exception e) {
            LOG.error(e);
        }
    }

    public void append(final Cell cell) {
        String rowFamilyCol = Bytes.toString(CellUtil.cloneRow(cell))+Bytes.toString(CellUtil.cloneFamily(cell))+Bytes.toString(CellUtil.cloneQualifier(cell));
        if (lastCell != null) {
            int keyComp = comparator.compareRows(lastCell, cell);
            if (keyComp != 0) {
                //there is a new row
                writeBufferedRow();
                bufferForRow = new HashMap<>();
                bufferForRow.put(rowFamilyCol,cell);
            } else {
                if(bufferForRow.containsKey(rowFamilyCol))
                {
                    Long timestamp = bufferForRow.get(rowFamilyCol).getTimestamp();
                    if(cell.getTimestamp()>timestamp)
                    {
                        bufferForRow.put(rowFamilyCol,cell);
                    }
                } else {
                    bufferForRow.put(rowFamilyCol,cell);
                }
            }
            lastCell = cell;
        } else {
            //First entry of the writer so initialize the structures
            lastCell = cell;
            bufferForRow = new HashMap<>();
            bufferForRow.put(rowFamilyCol,cell);
        }
    }

    private void writeBufferedRow() {
        if(bufferForRow==null) return;
        Group groupToWrite = new SimpleGroup(schema);
        Set<Map.Entry<String,Cell>> keySet = bufferForRow.entrySet();
        for (Map.Entry<String,Cell> cellToWriteEntry : keySet) {
            Cell cellToWrite = cellToWriteEntry.getValue();
            String family = Bytes.toString(CellUtil.cloneFamily(cellToWrite));
            String column = Bytes.toString(CellUtil.cloneQualifier(cellToWrite));
            if(column==null||column.length()==0) column="null";
            String fullColumnName = column;
            if(family!=null&&family.length()>0) fullColumnName = family+":"+column;
            Type parquetType = schema.getType(fullColumnName);
            byte[] value = CellUtil.cloneValue(cellToWrite);
            try {
                addToGroup(groupToWrite, parquetType, value, fullColumnName);
            } catch (Exception e) {
                LOG.error("Exception in "+path);
                e.printStackTrace();
            }
        }

        Type parquetType = schema.getType("key");
        byte[] key = CellUtil.cloneRow(lastCell);
        String columnName = "key";
        try {
            addToGroup(groupToWrite, parquetType, key, columnName);
        } catch (Exception e) {
            LOG.error("Exception in "+path);
            e.printStackTrace();
        }
        try {
            writer.write(groupToWrite);
        } catch (IOException e) {
            LOG.error(e);
        }
    }

    private void addToGroup(Group groupToWrite, Type parquetType, byte[] value, String columnName) {
        PrimitiveType.PrimitiveTypeName primitiveTypeName = parquetType.asPrimitiveType().getPrimitiveTypeName();
        if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT32)) {
            groupToWrite.add(columnName, Bytes.toInt(value));
        } else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.INT64)) {
            groupToWrite.add(columnName, Bytes.toLong(value));
        } else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.DOUBLE)) {
            groupToWrite.add(columnName, Bytes.toDouble(value));
        } else if (primitiveTypeName.equals(PrimitiveType.PrimitiveTypeName.BINARY)) {
            OriginalType originalType = parquetType.getOriginalType();
            if (originalType != null && originalType.equals(OriginalType.UTF8)) {
                groupToWrite.add(columnName, Bytes.toString(value));
            } else {
                groupToWrite.add(columnName, Binary.fromByteArray(value));
            }
        }
    }

    public void close() {
        writeBufferedRow();
        try {
            writer.close();
        } catch (Exception e) {
            LOG.error(e);
        }
    }
}