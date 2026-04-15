package edu.mcw.rgd.variantQc;

import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.process.MemoryMonitor;
import edu.mcw.rgd.process.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.*;

public class RemoveDuplicates {

    protected Logger logger = LogManager.getLogger("dupeStatus");
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private DAO dao = new DAO();
    private String version;
    private int mapKey;
    void run() throws Exception {

        logger.info(getVersion());
        logger.info("   " + dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at " + sdt.format(new Date(pipeStart)) + "\n");

        MemoryMonitor memoryMonitor = new MemoryMonitor();
        memoryMonitor.start();
        Map<Long, VariantMapData> origVars = new HashMap<>();
        List<VariantMapData> vars = dao.getDuplicateVariants(mapKey, origVars);
        List<VariantMapData> updateRs = new ArrayList<>();
        if (vars == null || vars.isEmpty()){
            memoryMonitor.stop();
            logger.info("No duplicates found!");
            return;
        }
        // all are duplicates of a previous variant
        for (VariantMapData v : vars){
            if (v.getRsId()!=null && !v.getRsId().isEmpty()) {
                String[] rsIds = v.getRsId().split(";");
                String rsId = rsIds[0];
                VariantMapData orig = origVars.get(v.getId());
                if (!Utils.stringsAreEqual(orig.getRsId(),rsId)){
                    orig.setRsId(rsId);
                    updateRs.add(orig);
                }
            }

        }
        // need to remove sample detail and withdraw

        if (!updateRs.isEmpty()){
            logger.info("Variants having updated rs Ids: "+updateRs.size());
            dao.updateVariant(updateRs);
        }

        logger.info("Variants being withdrawn and sample details being deleted: "+vars.size());
        dao.deleteSampleDetailsBatch(vars);
        dao.withdrawVariants(origVars.keySet());

        memoryMonitor.stop();

        logger.info(memoryMonitor.getSummary());

        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }
}
