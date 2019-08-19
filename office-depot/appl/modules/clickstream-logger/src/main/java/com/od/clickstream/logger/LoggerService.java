package com.od.clickstream.logger;
 
import com.google.common.base.Charsets;
import com.google.common.io.CharStreams;
import com.od.clickstream.constants.Constants;
import com.od.clickstream.facade.FlumeFacade;
import org.apache.commons.compress.utils.IOUtils;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.*;

/**
 * @author sandeep
 */
@Path("/clicklogger")
public class LoggerService {

	@Context
	private ServletContext context;

	private FlumeFacade client;

	@GET
	@Path("/{param}")
	public Response log(@PathParam("param") String msg) {
		return Response.status(Response.Status.OK).entity(msg).build();
	}

	@POST
	@Consumes(MediaType.WILDCARD)
	public Response receive(@Context HttpServletRequest request) {
		try {
			client = (FlumeFacade) context.getAttribute(Constants.ATTR_FLUME);
			if(null == client) {
				throw new RuntimeException(Constants.ERROR_CLIENT_NOT_INITIALIZED);
			}

//			final String payload = CharStreams.toString(new InputStreamReader(request.getInputStream(), Charsets.UTF_8));
			final String payload = getStringFromInputStream(request.getInputStream());
			System.out.println("****** " + payload);

			client.sendDataToFlume(payload);
			return Response.status(Response.Status.OK).entity(Constants.RESPONSE_SUCCESS).build();
		} catch (Exception e) {
			e.printStackTrace();
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(Constants.RESPONSE_ERROR).build();
		}
	}

	// convert InputStream to String
	private static String getStringFromInputStream(InputStream is) {
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();
		String line;
		try {
			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch(Exception e){
			}
		}
		return sb.toString();
	}
}