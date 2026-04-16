package edu.mcw.rgd.variantQc;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.OntologyXDAO;
import edu.mcw.rgd.dao.impl.RGDManagementDAO;
import edu.mcw.rgd.dao.impl.SampleDAO;
import edu.mcw.rgd.dao.impl.StrainDAO;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.dao.spring.SampleQuery;
import edu.mcw.rgd.dao.spring.variants.VariantMapQuery;
import edu.mcw.rgd.dao.spring.variants.VariantQuery;
import edu.mcw.rgd.dao.spring.variants.VariantSampleQuery;
import edu.mcw.rgd.datamodel.Eva;
import edu.mcw.rgd.datamodel.RgdId;
import edu.mcw.rgd.datamodel.Sample;
import edu.mcw.rgd.datamodel.Strain;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.*;

/**
 * Created by llamers on 1/28/2020.
 */
public class DAO {

    private OntologyXDAO xdao = new OntologyXDAO();
    private StrainDAO sdao = new StrainDAO();
    private VariantDAO vdao = new VariantDAO();
    private SampleDAO sampleDAO = new SampleDAO();

    public String getConnection(){
        return vdao.getConnectionInfo();
    }

    public DataSource getVariantDataSource() throws Exception{
        return DataSourceFactory.getInstance().getCarpeNovoDataSource();
    }

    public List<VariantSampleDetail> getVariantSampleDetail(int rgdId, int sampleId) throws Exception{
        String sql = "SELECT * FROM variant_sample_detail  WHERE rgd_id=? AND sample_id=?";
        VariantSampleQuery q = new VariantSampleQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(rgdId, sampleId);
    }

    public List<VariantMapData> getVariantsByPositionAndMapkey(int mapKey, String chr, int startPos)throws Exception{
        String sql = "SELECT * FROM variant v inner join variant_map_data vmd on v.rgd_id=vmd.rgd_id where vmd.map_key=? and vmd.chromosome=? and vmd.start_pos=?";
        VariantMapQuery q = new VariantMapQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.VARCHAR));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        return q.execute(mapKey,chr,startPos);
    }

    public Strain getStrainBySymbol(String symbol) throws Exception {
        return sdao.getStrainBySymbol(symbol);
    }

    public Strain getStrainByAlias(String symbol) throws Exception{
        return sdao.getActiveStrainByAlias(symbol,3);
    }

    public Strain getStrainByAliasWithLike(String symbol) throws Exception{
        return sdao.getActiveStrainByAliasUsingLike(symbol,3);
    }

    public Sample getSampleByStrainRgdIdAndMapKey(int strainRgdId, int mapKey) throws Exception {
        String sql = "SELECT * FROM sample WHERE strain_rgd_id=? and patient_id in (" +
                "select patient_id from patient where map_key=?)";
        SampleQuery q = new SampleQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        List<Sample> samples = q.execute(strainRgdId, mapKey);
        return samples.isEmpty() ? null : samples.get(0);
    }

    public Sample getSampleByStrainNameUsingLike(String symbol, int mapKey)throws Exception{
        String sql = "select * from sample where analysis_name like '%"+symbol+"%' and map_key=?";
        SampleQuery q = new SampleQuery(DataSourceFactory.getInstance().getCarpeNovoDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        List<Sample> samples = q.execute(mapKey);
        return samples.isEmpty() ? null : samples.get(0);
    }

    public Sample getSampleByAnalysisNameAndMapKey(String name, int mapKey) throws Exception{
        return sampleDAO.getSampleByAnalysisNameAndMapKey(name, mapKey);
    }

    public void updateGenicStatus(List<VariantMapData> mapsData) throws Exception {
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "update variant_map_data set GENIC_STATUS=? where RGD_ID=?",
                new int[]{Types.VARCHAR,Types.INTEGER});
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(v.getGenicStatus(),id);
        }
        sql2.flush();
    }

    public Sample getSampleByStrain(String strainSymbol, int mapKey) throws Exception{
        Strain s = getStrainBySymbol(strainSymbol);
        if (s==null) {
            s = getStrainByAlias(strainSymbol);
            if (s == null)
                s=getStrainByAliasWithLike(strainSymbol);
        }
        if (s==null)
            return null;

        return getSampleByStrainRgdIdAndMapKey(s.getRgdId(),mapKey);
    }

    public Sample getSampleBySampleId(int sampleId) throws Exception {
        String sql = "SELECT * FROM sample WHERE sample_id=?";
        SampleQuery q = new SampleQuery(getVariantDataSource(), sql);
        q.declareParameter(new SqlParameter(Types.INTEGER));
        q.compile();
        List<Sample> samples = q.execute(sampleId);
        return samples.isEmpty() ? null : samples.get(0);
    }

    public int insertSampleDetails(Collection<VariantSampleDetail> details) throws Exception {
        return vdao.insertVariantSample(details);
    }

    public int updateSampleDetails(Collection<VariantSampleDetail> details) throws Exception {
        return vdao.updateVariantSample(details);
    }

    public List<VariantMapData> getDuplicateVariants(int mapKey, Map<Long,VariantMapData> originals) throws Exception{
        String sql = "select v1.*, vm1.*, v2.rgd_id as rgd_id2, v2.rs_id as rs_id2 from variant v1, variant v2, variant_map_data vm1, variant_map_data vm2, rgd_ids r where " +
                "        NVL(v1.VAR_NUC,'-')=NVL(v2.VAR_NUC,'-') and NVL(v1.REF_NUC,'-')=NVL(v2.REF_NUC,'-') and v1.rgd_id=vm1.rgd_id and v2.rgd_id=vm2.rgd_id and r.rgd_id=v1.rgd_id and r.object_status='ACTIVE' and" +
                "        vm1.start_pos=vm2.start_pos and vm1.end_pos=vm2.end_pos and vm1.map_key=vm2.map_key and vm1.chromosome=vm2.chromosome and vm1.rgd_id>vm2.rgd_id and vm1.map_key=?";
//        VariantQuery q = new VariantQuery(getVariantDataSource(), sql);
//        q.declareParameter(new SqlParameter(Types.INTEGER));
//        q.compile();
//        List<VariantMapData> vars = q.execute(mapKey);
        List<VariantMapData> vars = new ArrayList<>();
        try (Connection con = vdao.getConnection()){
            PreparedStatement ps = con.prepareStatement(sql);
            ps.setInt(1, mapKey);
            ResultSet rs = ps.executeQuery();
            while (rs.next()){
                VariantMapData v1 = new VariantMapData();
                VariantMapData v2 = new VariantMapData();
                v1.setId(rs.getLong("RGD_ID"));
                v1.setChromosome(rs.getString("CHROMOSOME"));
                v1.setPaddingBase(rs.getString("PADDING_BASE"));
                v1.setStartPos(rs.getInt("START_POS"));
                v1.setEndPos(rs.getInt("END_POS"));
                v1.setGenicStatus(rs.getString("GENIC_STATUS"));
                v1.setReferenceNucleotide(rs.getString("REF_NUC"));
                v1.setVariantNucleotide(rs.getString("VAR_NUC"));
                v1.setVariantType(rs.getString("VARIANT_TYPE"));
                v1.setSpeciesTypeKey(rs.getInt("SPECIES_TYPE_KEY"));
                v1.setClinvarId(rs.getString("CLINVAR_ID"));
                v1.setRsId(rs.getString("RS_ID"));

                v2.setChromosome(v1.getChromosome());
                v2.setPaddingBase(v1.getPaddingBase());
                v2.setStartPos(v1.getStartPos());
                v2.setEndPos(v1.getEndPos());
                v2.setGenicStatus(v1.getGenicStatus());
                v2.setReferenceNucleotide(v1.getReferenceNucleotide());
                v2.setVariantNucleotide(v1.getVariantNucleotide());
                v2.setVariantType(v1.getVariantType());
                v2.setSpeciesTypeKey(v1.getSpeciesTypeKey());
                v2.setClinvarId(v1.getClinvarId());
                v2.setId(rs.getLong("RGD_ID2"));
                v2.setRsId(rs.getString("RS_ID2"));
                vars.add(v1);
                originals.put(v1.getId(), v2);
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        return vars;
    }

    public List<VariantMapData> getAllVariantByRsIdAndMapKey(String rsId, int mapKey) throws Exception {
        return vdao.getAllVariantByRsIdAndMapKey(rsId, mapKey);
    }

    public void updateVariant(List<VariantMapData> mapsData) throws Exception {
        BatchSqlUpdate sql2 = new BatchSqlUpdate(this.getVariantDataSource(),
                "update variant set RS_ID=? where RGD_ID=?",
                new int[]{Types.VARCHAR,Types.INTEGER});
        sql2.compile();
        for( VariantMapData v: mapsData) {
            long id = v.getId();
            sql2.update(v.getRsId(),id);
        }
        sql2.flush();
    }

    public int deleteSampleDetailsBatch(Collection<VariantMapData> tobeDeleted) throws Exception {
        BatchSqlUpdate su = new BatchSqlUpdate(vdao.getDataSource(),"DELETE FROM VARIANT_SAMPLE_DETAIL WHERE RGD_ID=?",
                new int[] {Types.INTEGER});

        for(VariantMapData v : tobeDeleted)
            su.update(v.getId());

        return vdao.executeBatch(su);
    }

    public void withdrawVariants(Collection<Long> tobeWithdrawn) throws Exception {
        RGDManagementDAO mdao = new RGDManagementDAO();
        for (Long rgdId : tobeWithdrawn){
            RgdId id = new RgdId(rgdId.intValue());
            mdao.withdraw(id);
        }
    }

    public void listFilesInFolder(File folder, ArrayList<File> vcfFiles) throws Exception {
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (file.isDirectory()) {
                listFilesInFolder(file,vcfFiles);
            } else {
                if (file.getName().endsWith(".vcf.gz")) {
//                    System.out.println(file.getName());
                    vcfFiles.add(file);
                }
            }
        }
    }
}
