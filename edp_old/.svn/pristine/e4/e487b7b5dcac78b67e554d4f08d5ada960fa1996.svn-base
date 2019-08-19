package com.cablevision.shoppingcart.dao;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cablevision.shoppingcart.CartProductListInfo;
import com.cablevision.shoppingcart.LogInfo;
import com.cablevision.shoppingcart.MissingProductOffering;
import com.cablevision.shoppingcart.ParamDTMInfo;
import com.cablevision.shoppingcart.ProductOfferingInfo;
import com.cablevision.shoppingcart.RateCodeInfo;

import oracle.jdbc.pool.OracleDataSource;

public class ShoppingCartDAO {
	
	private static final Logger logger = LoggerFactory.getLogger(ShoppingCartDAO.class);
	
	private Properties props;
	private Connection conn;
	private BigDecimal keyParamId;
	
	public ShoppingCartDAO(Properties props) throws IOException
	{
		this.props = props;
		this.keyParamId = BigDecimal.valueOf(Long.parseLong(props.getProperty("key_param_id")));
	}
	
	
	private Connection getConnection() throws Exception
	{
		if (conn != null && !conn.isClosed())
			return conn;
		
		OracleDataSource ds = new OracleDataSource();
		String host = props.getProperty("db.host");
		String port = props.getProperty("db.port");
		String userId = props.getProperty("db.userId");
		String password = props.getProperty("db.password");
		String sid = props.getProperty("db.sid");
		String connectionString = "jdbc:oracle:thin:"+userId+"/"+password+"@"+host+":"+port+":"+sid;
		String connectionString2 = "jdbc:oracle:thin:"+userId+"/########@"+host+":"+port+":"+sid;
		logger.info("connecting to database using connection string "+connectionString2);
		ds.setURL(connectionString);
		
		conn = ds.getConnection();
		
		return conn;
	}

	
	public void saveShoppingCartInfo(List<CartProductListInfo> cartProductInfoList, BigDecimal lastShoppingCartChangeSequenceId, 
										String sequenceId, java.sql.Timestamp dtmStartDate, java.sql.Timestamp dtmEndDate) throws Exception
	{
		logger.info("Saving shopping cart info into database");
		Connection con = null;
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		PreparedStatement ps3 = null;
		PreparedStatement ps4 = null;
		
		logger.info("updated dtm start date "+dtmStartDate+", dtm end date "+dtmEndDate);
		
		try {
			con = getConnection();
			logger.info("Got database connection");
			
			String query = "insert into f_cart_product_list values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
			String query2 = "insert into f_offer_id values (?,?,?,?,?,?,?,?)";
			String query3 = "update key_params set param_varchar = ?, param_dtm_start = ?, param_dtm_end = ?, dtm_last_updated = ? where key_param_id = ?";
			String query4 = "select dtm_last_updated_src from ecr9_shopping_cart where cart_id = ?";
			
			ps = con.prepareStatement(query);
			ps2 = con.prepareStatement(query2);
			ps3 = con.prepareStatement(query3);
			ps4 = con.prepareStatement(query4);
					
			for(CartProductListInfo cartProductListInfo: cartProductInfoList)
			{
				String cartId = cartProductListInfo.getCartId();
				Timestamp lastUpdatedDtmSrc = null;
				ps4.setString(1, cartId);
				ResultSet rs = ps4.executeQuery();
				while(rs.next())
				{
					lastUpdatedDtmSrc = rs.getTimestamp(1);
				}
				
				if (CollectionUtils.isNotEmpty(cartProductListInfo.getRateCodeList()))
				{
					for (RateCodeInfo rateCodeinfo: cartProductListInfo.getRateCodeList())
					{			
						ps.setString(1, cartId);
						ps.setTimestamp(14, lastUpdatedDtmSrc);
						ps.setTimestamp(15, new java.sql.Timestamp(System.currentTimeMillis()));
						ps.setString(2, sequenceId);

						ps.setString(3, rateCodeinfo.getChargeTypeX9());

						
						if (rateCodeinfo.getDurationX9() != null)
						{
							
							ps.setInt(4, rateCodeinfo.getDurationX9());
						}
						else
						{
							ps.setNull(4, java.sql.Types.NUMERIC);
						}
						

						ps.setString(5, rateCodeinfo.getPartTypeX9());

						ps.setString(6, rateCodeinfo.getPremiumServiceFlagX9());

						ps.setString(8, rateCodeinfo.getProductTypeX9());

						ps.setString(10, rateCodeinfo.getPromotionIndX9());

						ps.setString(11, rateCodeinfo.getRateCodeX9());

						ps.setString(12, rateCodeinfo.getStandardRateX9());
						
						
						if (rateCodeinfo.getWinDurationX9() != null)
						{
							
							ps.setInt(13, rateCodeinfo.getWinDurationX9());
						}
						else
						{
							ps.setNull(13, java.sql.Types.NUMERIC);
						}
						
						if (rateCodeinfo.getPriceX9() != null)
						{
							
							ps.setFloat(7, rateCodeinfo.getPriceX9());
						}
						else
						{
							ps.setNull(7, java.sql.Types.NUMERIC);
						}
						
						if (rateCodeinfo.getPromoPriceX9() != null)
						{
							
							ps.setFloat(9, rateCodeinfo.getPromoPriceX9());
						}
						else
						{
							ps.setNull(9, java.sql.Types.NUMERIC);
						}
						
						ps.addBatch();				
					}
				}
				
				
				if (CollectionUtils.isNotEmpty(cartProductListInfo.getProductOfferingList()))
				{
					for(ProductOfferingInfo productOffering: cartProductListInfo.getProductOfferingList())
					{
						ps2.setString(1, cartId);
						ps2.setString(2, sequenceId);
						ps2.setTimestamp(6, lastUpdatedDtmSrc);
						ps2.setTimestamp(7, new java.sql.Timestamp(System.currentTimeMillis()));

						ps2.setString(3, productOffering.getOfferId());
						ps2.setString(4, productOffering.getOfferName());
						ps2.setString(5, productOffering.getOfferDescription());
						
						ps2.setInt(8, productOffering.getOfferCount());
						ps2.addBatch();
					}
				}
			}

			ps3.setTimestamp(2, dtmStartDate);
			ps3.setTimestamp(3, dtmEndDate);
			ps3.setTimestamp(4, new java.sql.Timestamp(System.currentTimeMillis()));
			ps3.setString(1, lastShoppingCartChangeSequenceId.toString());
			ps3.setBigDecimal(5, keyParamId);
			logger.info("Committing shopping cart information into database");
			con.setAutoCommit(false);
			ps.executeBatch();
			ps2.executeBatch();
			ps3.execute();
			con.commit();
			logger.info("Committed shopping cart information into database");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(),e);
			
			try {
				con.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				logger.error(e1.getMessage(),e1);
			}
			
			throw e;
			
		} finally {
			try {
		          con.close();
		      } catch (SQLException e) {
		         logger.error(e.getMessage(), e);;
		      }
		}
		
	}
	
	public Map<BigDecimal, String> getModifiedShoppingCartIds(BigDecimal keyParamId) throws Exception
	{
		logger.info("Fetching cart ids of modified shopping carts");
		Connection con = null;
		PreparedStatement ps = null;
		Map<BigDecimal, String> cartIdMap = new HashMap<BigDecimal, String>();
		
		try {
			con = getConnection();
			String query = "select h_shopping_cart_id, cart_id from h_ecr9_shopping_cart where h_shopping_cart_id > (select param_varchar from key_params where key_param_id = ?) "
								+ "	order by h_shopping_cart_id";
			
			ps = con.prepareStatement(query);
			ps.setBigDecimal(1, keyParamId);
			ResultSet rs = ps.executeQuery();
			
			while (rs.next())
			{
				cartIdMap.put(rs.getBigDecimal(1), rs.getString(2));
			}
			
			logger.info("Fetched cart ids of modified shopping carts");
			return cartIdMap;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			throw e;
			
		} finally {
				try {
		          ps.close();
		          con.close();
		      } catch (SQLException e) {
		         logger.error(e.getMessage(), e);;
		      }
		}
		
	}
	
	public ParamDTMInfo getKeyParamDTMInfo(BigDecimal keyParamId) throws Exception
	{
		logger.info("Fetching keyparam info from database");
		Connection con = null;
		PreparedStatement ps = null;
		ParamDTMInfo paramDTMInfo = new ParamDTMInfo();

		try {
			con = getConnection();
			String query = "select param_dtm_start, param_dtm_end, param_numberic from key_params where key_param_id = ?";
			
			ps = con.prepareStatement(query);
			ps.setBigDecimal(1, keyParamId);
			ResultSet rs = ps.executeQuery();
			
			
			while (rs.next())
			{
				paramDTMInfo.setDtmStartDate(rs.getTimestamp(1));
				paramDTMInfo.setDtmEndDate(rs.getTimestamp(2));
				paramDTMInfo.setInterval(rs.getLong(3));
			}
			
			logger.info("Fetched keyparam info from database");
			return paramDTMInfo;
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			throw e;
			
		} finally {
				try {
					if (ps != null)
						ps.close();
					
					if (con != null)
						con.close();
					
		      } catch (SQLException e) {
		         logger.error(e.getMessage(), e);;
		      }
		}
	}
	
	public void saveStatisticsInfo(LogInfo logInfo, Map<String, MissingProductOffering> missingProductOfferingMap, String sequenceId, Map<String, BigDecimal> cartIdChangeSeqIdMap) throws Exception
	{
		logger.info("Saving log info into database");
		Connection con = null;
		PreparedStatement ps = null;
		PreparedStatement ps2 = null;
		
		try {
			con = getConnection();
			String query = "insert into web_service_log values (?,?,?,?,?,?,?,?,?,?)";
	
			ps = con.prepareStatement(query);		
			ps.setString(1, logInfo.getSequenceId());
			ps.setBigDecimal(2, logInfo.getKeyParamId());
			ps.setBigDecimal(3, logInfo.getH_shoppingCartIdBegin());
			ps.setBigDecimal(4, logInfo.getH_shoppingCartIdEnd());
			ps.setLong(5, logInfo.getCartIdTotalRequesCount());
			ps.setLong(6, logInfo.getCartIdSuccessRequestCount());
			ps.setLong(7, logInfo.getCartIdNoResponseCount());
			ps.setTimestamp(8,  logInfo.getDtmStartTime());
			ps.setTimestamp(9,  logInfo.getDtmEndTime());
			ps.setTimestamp(10,  logInfo.getDtmCreatedTime());
			
			ps.execute();
			logger.info("Saved shopping cart statistics info into database");
			
			String query2 = "insert into f_cart_missing_info values (?,?,?,?,?)";
			ps2 = con.prepareStatement(query2);
			
			for(Map.Entry<String, MissingProductOffering> entry: missingProductOfferingMap.entrySet())
			{
				ps2.setBigDecimal(1, cartIdChangeSeqIdMap.get(entry.getKey()));
				ps2.setString(2, sequenceId);
				ps2.setString(3, entry.getKey());
				ps2.setInt(4, entry.getValue().isProductNull() ? 1 : 0);
				ps2.setInt(5, entry.getValue().isOfferNull() ? 1 : 0);
				
				ps2.addBatch();
			}
			
			ps2.executeBatch();
			logger.info("Saved missing productoffering statistics info into database");
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
			throw e;
			
		} finally {
				try {
		          ps.close();
		          con.close();
		      } catch (SQLException e) {
		         logger.error(e.getMessage(), e);;
		      }
		}
	}
	
	public void closeConnection()
	{
		try {
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
		}
	}

}
