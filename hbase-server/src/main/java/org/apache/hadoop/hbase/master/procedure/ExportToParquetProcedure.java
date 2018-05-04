package org.apache.hadoop.hbase.master.procedure;

import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.ipc.HBaseRpcController;
import org.apache.hadoop.hbase.procedure2.ProcedureSuspendedException;
import org.apache.hadoop.hbase.procedure2.ProcedureYieldException;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

@InterfaceAudience.Public
public class ExportToParquetProcedure extends AbstractStateMachineTableProcedure<ExportToParquetProcedure.ExportToParquetProcedureState> {

    private static final Logger LOG = LoggerFactory.getLogger(ExportToParquetProcedure.class);
    private TableDescriptor tableToBeExportedDescriptor;

    public ExportToParquetProcedure(){

    }

    public ExportToParquetProcedure(
            final MasterProcedureEnv env,
            final TableDescriptor htd){
        super(env);
        this.tableToBeExportedDescriptor = htd;
    }

    @Override
    public TableName getTableName() {
        return tableToBeExportedDescriptor.getTableName();
    }

    @Override
    public TableOperationType getTableOperationType() {
        return TableOperationType.EXPORT_PARQUET;
    }

   @Override
    protected Flow executeFromState(MasterProcedureEnv env, ExportToParquetProcedureState state) throws ProcedureSuspendedException, ProcedureYieldException, InterruptedException {
        HBaseRpcController controller = env.getMasterServices().getClusterConnection().getRpcControllerFactory().newController();
        try {
            switch (state) {
                case START:
                    Connection connection = env.getMasterServices().getConnection();
                    List<Pair<RegionInfo, ServerName>> existingTableRegions =
                            MetaTableAccessor.getTableRegionsAndLocations(connection, getTableName());
                    for(Pair<RegionInfo, ServerName> regionInfoName : existingTableRegions){
                        HBaseProtos.RegionSpecifier regionSpecifier = RequestConverter.buildRegionSpecifier(
                                HBaseProtos.RegionSpecifier.RegionSpecifierType.REGION_NAME,regionInfoName.getFirst().getRegionName());
                        AdminProtos.ExportRegionToParquetRequest request = AdminProtos.ExportRegionToParquetRequest.newBuilder().setRegion(regionSpecifier).build();
                        env.getMasterServices().getServerManager().getRsAdmin(regionInfoName.getSecond()).exportRegionToParquet(controller,request);
                    }
                    setNextState(ExportToParquetProcedureState.FINISH);
                    break;
                case FINISH:
                    LOG.info("Finished");
                    return Flow.NO_MORE_STATE;
            }
        }catch (Exception e) {
            LOG.warn(this + "; Failed state=" + state + ", retry " + this + "; cycles=" +
                    getCycles(), e);
        }
        return Flow.HAS_MORE_STATE;
    }

    @Override
    protected void rollbackState(MasterProcedureEnv env, ExportToParquetProcedureState exportToParquetProcedureState) throws IOException, InterruptedException {
        throw new UnsupportedOperationException("unhandled state=" + exportToParquetProcedureState);
    }

    @Override
    protected ExportToParquetProcedureState getState(int stateId) {
        if(stateId==1) return ExportToParquetProcedureState.START;
        else if(stateId==2) return ExportToParquetProcedureState.FINISH;
        return ExportToParquetProcedureState.START;
    }

    @Override
    protected int getStateId(ExportToParquetProcedureState exportToParquetProcedureState) {
        return exportToParquetProcedureState.getStateId();
    }

    @Override
    protected ExportToParquetProcedureState getInitialState() {
        return ExportToParquetProcedureState.START;
    }

    public enum ExportToParquetProcedureState {
        START(1), FINISH(2);

        int stateId;

        ExportToParquetProcedureState(int stateId){
            this.stateId = stateId;
        }

        public int getStateId() {
            return stateId;
        }
    }

}
