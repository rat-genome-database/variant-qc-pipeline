package edu.mcw.rgd.variantQc;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.FileDownloader;
import edu.mcw.rgd.process.Utils;
import edu.mcw.rgd.util.Zygosity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.zip.GZIPInputStream;

public class VariantSampleQC {

    private String version;
    private int mapKey;
    private String qcFile;
    private Map<String, Integer> diffSampleNameMap;

    protected Logger logger = LogManager.getLogger("status");
    protected Logger notFound = LogManager.getLogger("notfoundStatus");
    protected Logger noSample = LogManager.getLogger("noSampleStatus");
    private DAO dao = new DAO();
    private Zygosity zygosity = new Zygosity();
    private SimpleDateFormat sdt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


    void main() throws Exception{
        logger.info(getVersion());
        logger.info("   "+dao.getConnection());

        long pipeStart = System.currentTimeMillis();
        logger.info("Pipeline started at "+sdt.format(new Date(pipeStart))+"\n");

        try (BufferedReader br = openFile(qcFile)){
            String lineData;
            String[] cols = {};
            HashMap<Integer, Sample> sampleMap = new HashMap<>();
            int totalNewSamples = 0, varNotFound = 0;
            List<VariantSampleDetail> tobeUpdated = new ArrayList<>();
            List<VariantSampleDetail> tobeInserted = new ArrayList<>();
            int totalUpdated = 0;
            while ( (lineData = br.readLine()) != null )
            {
//                Map<VariantMapData,String>
                if (lineData.startsWith("##"))
                    continue;
                if (lineData.startsWith("#")){
                    cols = lineData.split("\t");
                    for (int i = 9; i < cols.length; i++){
                        // get strain rgd ids and get sample ID
                        Sample s = dao.getSampleByStrain(cols[i], mapKey);
                        if (s==null){
                            s = dao.getSampleByStrainNameUsingLike(cols[i], mapKey);
                            if (s==null) {
                                Integer sampleFromMap = diffSampleNameMap.get(cols[i]);
                                if (sampleFromMap!=null)
                                    s = dao.getSampleBySampleId(sampleFromMap);
                            }
                        }
                        if (s==null)
                            logger.info("\tStrain not found: " + cols[i]);
                        sampleMap.put(i,s);
                    }
                }
                else{
                    /*
                     * 0 chrom
                     * 1 pos
                     * 2 rid
                     * 3 ref
                     * 4 alt
                     * 5 qual
                     * 6 filter
                     * 7 info
                     * 8 format
                     * 9 - 50 Strains
                     * */
                    String[] lineSplit = lineData.split("\t");
                    String chrom = "";
                    int pos = 0;
                    String ref = "";
                    String alt = "";
                    String[] alts = {};
                    for (int i = 0; i < 9; i++){
                        switch (i){
                            case 0: // chrom
                                chrom = lineSplit[i];
                                if (chrom.equals("chrM"))
                                    chrom = chrom.replace("chrM","MT");
                                if (chrom.startsWith("chr") || chrom.startsWith("CHR") || chrom.startsWith("Chr")) {
                                    chrom = chrom.replace("chr","").replace("CHR", "").replace("Chr","");
                                }
                                break;
                            case 1: // pos
                                pos = Integer.parseInt(lineSplit[i]);
                                break;
                            case 2: // id
                                break;
                            case 3: // ref
                                ref = lineSplit[i];
                                break;
                            case 4: // alt
                                alt = lineSplit[i];
                                alts = lineSplit[i].split(",");
                                break;
                            case 5: // qual
                                break;
                            case 6: // filter
                                break;
                            case 7: // info
                                break;
                            case 8: // format
                                break;
                        }
                    }
                    // get variant
                    List<VariantMapData> vmds = dao.getVariantsByPositionAndMapkey(mapKey,chrom,pos);
                    VariantMapData var = null;
                    Map<Integer, VariantMapData> variantMap = new HashMap<>();
                    for(VariantMapData vmd : vmds){
                        if (alts.length>1) {
                            for (int j = 0; j < alts.length; j++) {
                                String a = alts[j];
                                String r = ref;
//                                if (r.length()>a.length()){
//                                    r = r.substring(1);
//                                    a = null;
//                                } else if (r.length() < a.length()) {
//                                    a = a.substring(1);
//                                    r = null;
//                                }
                                if (Utils.stringsAreEqual(vmd.getReferenceNucleotide(), r) && Utils.stringsAreEqual(vmd.getVariantNucleotide(), a)) {
                                    variantMap.put(j+1,vmd);
                                }
                            }
                        }
                        else {
                            String a = alt;
                            String r = ref;
//                            if (r.length()>a.length()){
//                               r = r.substring(1);
//                               a = null;
//                            } else if (r.length() < a.length()) {
//                                a = a.substring(1);
//                                r = null;
//                            }
                            if (Utils.stringsAreEqual(vmd.getReferenceNucleotide(), r) && Utils.stringsAreEqual(vmd.getVariantNucleotide(), a)) {
                                var = vmd;
                            }
                        }
                    }
                    Set<Integer> varKeys = variantMap.keySet();
                    if (var == null && variantMap.isEmpty()) {
//                        logger.info("Variant not found!");
                        notFound.info("CHR: "+chrom+"|POS: "+pos+"|Ref: "+ref+"|Alt: "+alt);
                        varNotFound++;
                        continue;
                    } else if (var == null && varKeys.size() < alts.length){
                        for (int j = 0; j < alts.length; j++){
                            if (!varKeys.contains(j+1)) {
//                                logger.info("Variant not found!");
                                notFound.info("CHR: " + chrom + "|POS: " + pos + "|Ref: " + ref + "|Alt: " + alts[j]);
                                varNotFound++;
                            }
                        }
                    }
                    for (int i = 9; i < lineSplit.length; i++){
                        // get depth and allele reads (var_freq) and compare to DB
                        // GT:AD:DP:GQ:PL  1/1:1,14,0:15:35:573,35,0,574,42,579
                        boolean updateMe = false;
                        Sample s = sampleMap.get(i);
                        if (s==null)
                            continue;
                        if (lineSplit[i].contains(":.:.:"))
                            continue;
                        String[] detailsSplit = lineSplit[i].split(":");
                        if (detailsSplit[0].equals("./."))
                            continue;
                        int totalDepth = Integer.parseInt(detailsSplit[2]); // 15 from example
                        if (totalDepth==0)
                            continue;
                        String[] alleleDepthSplit = detailsSplit[1].split(",");
                        if (!variantMap.isEmpty()) {
                            for (Integer varKey : varKeys){
                                VariantMapData v = variantMap.get(varKey);
                                List<VariantSampleDetail> detailList = getSampleDetail(variantMap.get(varKey),s);
                                int alleleDepth = Integer.parseInt(alleleDepthSplit[varKey]);
                                if (alleleDepth==0)
                                    continue;

                                if (detailList==null || detailList.isEmpty()){
                                    // find out why there is no sample detail
                                    noSample.info("No details: "+
                                            v.getId()+"\t"+v.getChromosome()+"\t"+v.getStartPos()+"|Sample: "+s.getId()+" "+lineSplit[i]);
                                    VariantSampleDetail vs = new VariantSampleDetail();
                                    vs.setSampleId(s.getId());
                                    vs.setDepth(totalDepth);
                                    vs.setVariantFrequency(alleleDepth);
                                    int zygPercentRead = vs.getVariantFrequency() / vs.getDepth();
                                    vs.setZygosityPercentRead(zygPercentRead);
                                    vs.setId(v.getId());
                                    zygosity.computeZygosityStatus(vs.getVariantFrequency(),vs.getDepth(),s.getGender(),v, vs);
                                    tobeInserted.add(vs);
                                    continue;
                                }
                                if (detailList.size()>1){
                                    // why are there multiple
                                    notFound.info("Many details\n"+v.getId()+"\t"+v.getChromosome()+"\t"+v.getStartPos()+"|Sample: "+s.getId());
                                }
                                VariantSampleDetail vsd = detailList.get(0);

                                if (vsd.getDepth()!=totalDepth){
                                    vsd.setDepth(totalDepth);
                                    updateMe = true;
                                }
                                if (vsd.getVariantFrequency() != alleleDepth){
                                    vsd.setVariantFrequency(alleleDepth);
                                    updateMe = true;
                                }

                                if (updateMe){
                                    int zygPercentRead = vsd.getVariantFrequency() / vsd.getDepth();
                                    vsd.setZygosityPercentRead(zygPercentRead);
                                    tobeUpdated.add(vsd);
                                }

                            }
                        }
                        else {
                            assert var != null;
                            List<VariantSampleDetail> detailList = getSampleDetail(var,s);
                            int alleleDepth = Integer.parseInt(alleleDepthSplit[1]);
                            if (alleleDepth==0)
                                continue;

                            if (detailList==null || detailList.isEmpty()){
                                // find out why there is no sample detail
                                noSample.info("No details: "+var.getId()+"\t"+var.getChromosome()+"\t"+var.getStartPos()+"|Sample: "+s.getId()+" "+lineSplit[i]);
                                // create sample
                                VariantSampleDetail vs = new VariantSampleDetail();
                                vs.setSampleId(s.getId());
                                vs.setDepth(totalDepth);
                                vs.setVariantFrequency(alleleDepth);
                                int zygPercentRead = vs.getVariantFrequency() / vs.getDepth();
                                vs.setZygosityPercentRead(zygPercentRead);
                                vs.setId(var.getId());
                                zygosity.computeZygosityStatus(vs.getVariantFrequency(),vs.getDepth(),s.getGender(),var, vs);
                                tobeInserted.add(vs);
                                continue;
                            }
                            if (detailList.size()>1){
                                // why are there multiple
                                notFound.info("Many details\n"+var.getId()+"\t"+var.getChromosome()+"\t"+var.getStartPos()+"|Sample: "+s.getId());
                            }
                            VariantSampleDetail vsd = detailList.get(0);

                            if (vsd.getDepth()!=totalDepth){
                                vsd.setDepth(totalDepth);
                                updateMe = true;
                            }
                            if (vsd.getVariantFrequency() != alleleDepth){
                                vsd.setVariantFrequency(alleleDepth);
                                updateMe = true;
                            }

                            if (updateMe){
                                int zygPercentRead = vsd.getVariantFrequency() / vsd.getDepth();
                                vsd.setZygosityPercentRead(zygPercentRead);
                                tobeUpdated.add(vsd);
                            }
                        }
                    } // end strains for

                }
                // check size of samples and update
                if (tobeUpdated.size()>=50000){
                    // dao call for updating sample details
                    dao.updateSampleDetails(tobeUpdated);
                    totalUpdated += tobeUpdated.size();
                    tobeUpdated.clear();
                }
                if (tobeInserted.size()>50000){
                    // insert samples
                    dao.insertSampleDetails(tobeInserted);
                    totalNewSamples += tobeInserted.size();
                    tobeInserted.clear();
                }
            } // end buffer reader
            if (tobeUpdated.size()>0) { // update remaining
                dao.updateSampleDetails(tobeUpdated);
                totalUpdated += tobeUpdated.size();
                logger.info("\tTotal samples being updated: "+totalUpdated);
            }
            if (tobeInserted.size()>0){ // insert remaining
                totalNewSamples += tobeInserted.size();
                logger.info("\tTotal samples being created: "+totalNewSamples);
                dao.insertSampleDetails(tobeInserted);
            }
        }
        catch (Exception e){
            Utils.printStackTrace(e, logger);
        }

        logger.info(" Total Elapsed time -- elapsed time: "+
                Utils.formatElapsedTime(pipeStart,System.currentTimeMillis())+"\n");
    }


    BufferedReader openFile(String fileName) throws IOException {

        String encoding = "UTF-8"; // default encoding

        InputStream is;
        if( fileName.endsWith(".gz") ) {
            is = new GZIPInputStream(new FileInputStream(fileName));
        } else {
            is = new FileInputStream(fileName);
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(is, encoding));
        return reader;
    }

    boolean isGenic(int start, int stop, String chr) throws Exception {

        GeneCache geneCache = geneCacheMap.get(chr);
        if( geneCache==null ) {
            geneCache = new GeneCache();
            geneCacheMap.put(chr, geneCache);
            geneCache.loadCache(38, chr, DataSourceFactory.getInstance().getDataSource());
        }
        List<Integer> geneRgdIds = geneCache.getGeneRgdIds(start,stop);
        return !geneRgdIds.isEmpty();
    }
    List<Integer> getGenesWithGeneCache(int start, int stop, String chr) throws Exception {

        GeneCache geneCache = geneCacheMap.get(chr);
        if( geneCache==null ) {
            geneCache = new GeneCache();
            geneCacheMap.put(chr, geneCache);
            geneCache.loadCache(38, chr, DataSourceFactory.getInstance().getDataSource());
        }
        List<Integer> geneRgdIds = geneCache.getGeneRgdIds(start,stop);
        return geneRgdIds;
    }

    List<VariantSampleDetail> getSampleDetail(VariantMapData vmd, Sample s) throws Exception{
        return dao.getVariantSampleDetail((int)vmd.getId(),s.getId());
    }


    Map<String, GeneCache> geneCacheMap;

    public void setVersion(String version) {
        this.version = version;
    }

    public String getVersion() {
        return version;
    }

    public void setMapKey(int mapKey) {
        this.mapKey = mapKey;
    }

    public int getMapKey() {
        return mapKey;
    }

    public void setQcFile(String qcFile) {
        this.qcFile = qcFile;
    }

    public String getQcFile() {
        return qcFile;
    }

    public void setDiffSampleNameMap(Map<String,Integer> diffSampleNameMap) {
        this.diffSampleNameMap = diffSampleNameMap;
    }

    public Map<String,Integer> getDiffSampleNameMap() {
        return diffSampleNameMap;
    }
}
