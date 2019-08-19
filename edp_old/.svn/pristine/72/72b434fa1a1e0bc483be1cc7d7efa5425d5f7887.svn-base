package com.cv.bis.security.exception;

/**
 * 
 * @author Q4
 *
 */
@SuppressWarnings("serial")
public class CVSecurityException extends CVApplicationException {

	int causeCode = 0;
	
	public CVSecurityException() {
		super();
	}
	
	public CVSecurityException(int code){
		this.causeCode = code;
	}
	
	public int getCauseCode(){
		return this.causeCode;
	}
	
	@Override
	public String getMessage() {
		String result = "CVSecurity utility encountered an unknown exception.";
		switch (causeCode) {
			case SYSTEM_INIT_ERROR: result="Error Initializing Security Controller"; break;
			case PROPERTIES_LOAD_ERROR: result="Error loading properties from file"; break;
			case SECURITY_UTIL_ERROR: result="Error Initializing Security Util"; break;
			case SECURITY_ALGORITHM_ERROR: result="Unknown secruity algorithm"; break;
			case SECURITY_KEY_ACCESS_ERROR: result="Unable to access database to aquire keys"; break;
			case SECURITY_KEY_DRIVER_ERROR: result="Database driver not found when trying to access Redshift."; break;
			case SECURITY_KEY_DB_GET_ERROR: result="Error encountered when trying to get key map from Redshift."; break;
			case UNKNOWN_USER: result="User parameter supplied does not match command execution user"; break;
			case UNKNOWN_ERROR: result="Unknown security utility error"; break;
			default:break;
		}
		return result;
	}

}
