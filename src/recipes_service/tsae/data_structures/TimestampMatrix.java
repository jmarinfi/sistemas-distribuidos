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

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class TimestampMatrix implements Serializable{
	
	private static final long serialVersionUID = 3331148113387926667L;
	ConcurrentHashMap<String, TimestampVector> timestampMatrix = new ConcurrentHashMap<String, TimestampVector>();
	
	public TimestampMatrix(List<String> participants){
		// create and empty TimestampMatrix
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			timestampMatrix.put(it.next(), new TimestampVector(participants));
		}
	}
	
	/**
	 * @param node
	 * @return the timestamp vector of node in this timestamp matrix
	 */
	TimestampVector getTimestampVector(String node){
		return timestampMatrix.get(node);
	}
	
	/**
	 * Merges two timestamp matrix taking the elementwise maximum
	 * @param tsMatrix
	 */
	public synchronized void updateMax(TimestampMatrix tsMatrix){
		for (String node: timestampMatrix.keySet()) {
			TimestampVector localVector = timestampMatrix.get(node);
			TimestampVector incomingVector = tsMatrix.getTimestampVector(node);
			localVector.updateMax(incomingVector);
		}
	}
	
	/**
	 * substitutes current timestamp vector of node for tsVector
	 * @param node
	 * @param tsVector
	 */
	public synchronized void update(String node, TimestampVector tsVector){
		timestampMatrix.replace(node, tsVector);
	}
	
	/**
	 * 
	 * @return a timestamp vector containing, for each node, 
	 * the timestamp known by all participants
	 */
	public TimestampVector minTimestampVector(){
		TimestampVector minTsVector = null;
		for (Enumeration<String> e = timestampMatrix.keys(); e.hasMoreElements();) {
			String node = e.nextElement();
			if (minTsVector == null) {
				minTsVector = timestampMatrix.get(node).clone();
			}
			else {
				minTsVector.mergeMin(timestampMatrix.get(node));
			}
		}
		return minTsVector;
	}
	
	/**
	 * clone
	 */
	public synchronized TimestampMatrix clone(){
		TimestampMatrix clonedTsMatrix = new TimestampMatrix(Arrays.asList(timestampMatrix.keySet().toArray(new String[0])));
		for (String node: timestampMatrix.keySet()) {
			clonedTsMatrix.timestampMatrix.put(node, timestampMatrix.get(node).clone());
		}
		return clonedTsMatrix;
	}
	
	/**
	 * equals
	 */
	@Override
	public synchronized boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || !(obj instanceof TimestampMatrix)) {
			return false;
		}
		TimestampMatrix other = (TimestampMatrix) obj;
		return timestampMatrix.equals(other.timestampMatrix);
	}

	
	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampMatrix==null){
			return all;
		}
		for(Enumeration<String> en=timestampMatrix.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampMatrix.get(name)!=null)
				all+=name+":   "+timestampMatrix.get(name)+"\n";
		}
		return all;
	}
}
