package edu.mcw.rgd.variantQc;

import edu.mcw.rgd.dao.DataSourceFactory;
import edu.mcw.rgd.dao.impl.*;
import edu.mcw.rgd.dao.impl.variants.VariantDAO;
import edu.mcw.rgd.dao.spring.SampleQuery;
import edu.mcw.rgd.dao.spring.variants.VariantMapQuery;
import edu.mcw.rgd.dao.spring.variants.VariantSampleQuery;
import edu.mcw.rgd.datamodel.*;
import edu.mcw.rgd.datamodel.ontologyx.Term;
import edu.mcw.rgd.datamodel.variants.VariantMapData;
import edu.mcw.rgd.datamodel.variants.VariantSampleDetail;
import edu.mcw.rgd.process.Utils;
import oracle.jdbc.proxy.annotation.Pre;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.SqlParameter;
import org.springframework.jdbc.object.BatchSqlUpdate;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    public int insertSampleDetails(Collection<VariantSampleDetail> details) throws Exception {
        return vdao.insertVariantSample(details);
    }

    public int updateSampleDetails(Collection<VariantSampleDetail> details) throws Exception {
        return vdao.updateVariantSample(details);
    }

}
