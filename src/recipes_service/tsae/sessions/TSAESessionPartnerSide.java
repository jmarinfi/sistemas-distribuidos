/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.sessions;


import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

import javax.sql.rowset.spi.SyncResolver;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.OperationType;
import recipes_service.data.Recipe;
import recipes_service.data.RemoveOperation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

import lsim.library.api.LSimLogger;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread{
	
	private Socket socket = null;
	private ServerData serverData = null;
	
	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// receive request from originator and update local state
			// receive originator's summary and ack
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] TSAE session");
			LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
			if (msg.type() == MsgType.AE_REQUEST){
				// JMF: get summary and acknowledgement from originator side
				TimestampVector originatorSummary = ((MessageAErequest) msg).getSummary();
				TimestampMatrix originatorAck = ((MessageAErequest) msg).getAck();
				
	            // send operations
				// JMF: send newer operations to originator side
				List<Operation> opsToSend = serverData.getLog().listNewer(originatorSummary);
				for (Operation operation: opsToSend) {
					msg = new MessageOperation(operation);
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);
				}

				// send to originator: local's summary and ack
				// JMF: This code block uses the synchronized keyword to ensure thread safety 
				// while accessing to serverData. It retrieves a copy of the summary from the data structure, 
				// updates the acknowledgement information, and creates a clone of the acknowledgement object.
				TimestampVector localSummary = null;
				TimestampMatrix localAck = null;
				synchronized (serverData) {
					localSummary = serverData.getSummary().clone();
					String hostId = serverData.getId();
					serverData.getAck().update(hostId, localSummary);
					localAck = serverData.getAck().clone();
				}
				
				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(current_session_number);
	 	        out.writeObject(msg);
				LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);

	            // receive operations
				List<Operation> opsReceived = new Vector<Operation>();
				msg = (Message) in.readObject();
				LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				while (msg.type() == MsgType.OPERATION){
					Operation op = ((MessageOperation) msg).getOperation();
					// JMF: Add the operation to the log and update data structures
					synchronized (serverData) {
						serverData.setPendingOperations(true);
						opsReceived.add(op);
						serverData.getLog().add(op);
					}
					msg = (Message) in.readObject();
					LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] received message: "+ msg);
				}
				
				// receive message to inform about the ending of the TSAE session
				LSimLogger.log(Level.TRACE, "[TSAESessionOriginatorSide] [session: "+current_session_number+"] received message: "+msg);
				if (msg.type() == MsgType.END_TSAE){
					// send and "end of TSAE session" message
					// JMF: Update the summary and ack, purge the log and execute the operations
					synchronized (serverData) {
						serverData.getSummary().updateMax(originatorSummary);
						serverData.getAck().updateMax(originatorAck);
						serverData.getLog().purgeLog(serverData.getAck());
						serverData.executeOperations(opsReceived);
					}
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
		            out.writeObject(msg);					
					LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] sent message: "+ msg);
				}
				
			}
			socket.close();		
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			LSimLogger.log(Level.FATAL, "[TSAESessionPartnerSide] [session: "+current_session_number+"]" + e.getMessage());
			e.printStackTrace();
            System.exit(1);
		}catch (IOException e) {
	    }
		
		LSimLogger.log(Level.TRACE, "[TSAESessionPartnerSide] [session: "+current_session_number+"] End TSAE session");
	}
}
