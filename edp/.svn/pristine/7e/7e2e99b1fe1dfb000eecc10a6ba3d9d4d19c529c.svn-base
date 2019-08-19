package test.security;

import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;

public class CVSecurityTest {

	public static void main(String[] args) {
		try {
			// To test encryption / decryption
			CVDataController cvc = new CVDataController("PRGBUY", "vpesara1", "cvlhdpdev");

			String encryptField = cvc.encryptField("HOUSEHOLD_ID", "Testing");
			System.out.println("encrypt::" + encryptField);
			String decryptField = cvc.decryptField("HOUSEHOLD_ID",
					"CB8CF70DBA535CDBFEB5B2885C8C0712");
			System.out.println("De-crypt::" + decryptField);

			// System.out.println("decrypt::"+cvc.decryptField("device",encryptField));

			CVDataController cvc1 = new CVDataController("OMNIAC", "vpesara1", "cvlhdpdev");
			String encryptField1 = cvc1.decryptAcctNumber("ACC_NUM",
					"EyxY89R5TBKHY5NhUIjIEdAkwAtL1qdtIEE9QwphEjQ=");
			System.out.println("encryptOMNIAC::" + encryptField1);
			
			
			
		
	 

			CVDataController cvc2 = new CVDataController("OMNIUT", "vpesara1", "cvlhdpdev");
			String encryptField2 = cvc2.decryptOmnitureCryptoUtil("ACC_NUM",
					"JeMBR+Rm9iIAsrGdOmKSag==");
			System.out.println("encryptOMNIUT::" + encryptField2);

			
	
			
			
			
			
			CVDataController cvc3 = new CVDataController("OMNIBZ", "vpesara1", "cvlhdpdev");
			String encryptField3 = cvc3.decryptOmnitureCryptoUtilBizz(
					"ACC_NUM", "3JkYg9XE19gLuUq3njVjaw==");
			System.out.println("encryptOMNIBZ::" + encryptField3);
			
	
			
            
			CVDataController cvc4 = new CVDataController("OMNIUTS", "vpesara1", "cvlhdpdev");
			String encryptField4 = cvc4.decryptOmnitureCipherUtils( 
					"ACC_NUM", "012ef02d6d45de4aaed5c6d2d53b46d1");
			System.out.println("encryptOMNIUTS::" + encryptField4);
			
			
		 
			

			CVDataController cvc5 = new CVDataController("GLOBAL_UNIFIED", "vpesara1", "cvlhdpdev");
			String encryptField5 = cvc5.decryptOmnitureCryptoUtil("OPTIMUM_ID","1J1yx1l0EAihVLCOi1WraQ");
			System.out.println("OPTIMUM_ID::" + encryptField5);
			
			
			//CVDataController cvc6 = new CVDataController("GLOBAL_UNIFIED", "vpesara1");
			
			 CVDataController cvc6 = new CVDataController("CHANNEL900", "vpesara1", "cvlhdpdev");
			
			String[]  home_id_list = {"02aEBAWEhIbA==",
					"02aEBAWFxQWA==",
					"02aEBAWGhoVA==",
					"02aEBAXGhsaA=="};
			String[]  mac_id_list = { "02TExIXMKVbMKVaEhEwpnCmQ==",
					"02bExIbMKJUMKJSEjCncKZwoE=",
					"02bExIbMKJUFBUSMKJwp3CmQ==",
					"02bExIbMKJUFREUFhMSA==",
					"02bExIbMKJUEBQaMKZwolY"};
	
		 for (int i=0 ; i <home_id_list.length;i++)		
			 System.out.println(cvc6.deObfuscateChannel900("HOME_ID",home_id_list[i]));	 
	    System.out.println("----------------------------------------------");
		 for ( int i=0 ; i < mac_id_list.length;i++)		
			 System.out.println(cvc6.deObfuscateChannel900("MAC_ID",mac_id_list[i]));	
			
		  System.out.println("++++"+cvc6.encryptField("MAC_ID", "0014F8F912EE"));
			/*CVDataController cvc5 = new CVDataController("GLOBAL_UNIFIED", "vpesara1");
			String encryptField5 = cvc5.decryptAcctNumber("OPTIMUM_ID","34RplGqYCSoAsrGdOmKSag");
			System.out.println("encryptOMNIAC::" + encryptField4);
			
			1J0G2SRbEhn9MikHld3z4Q
1J1yx1l0EAihVLCOi1WraQ
1J2WbYgBQoNA-DMPxE4aPQ
1J3jBHCVhXQAsrGdOmKSag
1J5HpfiGT8-3-vPTw5Hnlw
1JC6cxiwDkTdEAhBFLFQtw
1JC6fDNam58Qg1R9qCh1Vw
1JCChsaQHwAAsrGdOmKSag
1JE0I4rusicl0hzFDazSpA
1JECLuFX6y0CnbJA84LVuA
 

			*
			*
			*/
			
			
		
			
			
			
			
		} catch (CVApplicationException e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}

	}

}
