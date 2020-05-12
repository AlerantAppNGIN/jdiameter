package org.jdiameter.server.impl.io.sctp;

import static java.lang.Class.forName;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.concurrent.ConcurrentHashMap;

import org.mobicents.protocols.api.Management;

public class SctpServerManagementFactory {

	private static ConcurrentHashMap<String, Management> managementImplStore = new ConcurrentHashMap<String, Management>();;

	static synchronized Management createOrGetManagement(String stackName,
			String usedManagementImplementation) throws IOException {

		Management managmentImplInstance = managementImplStore.get(stackName);
		if (managmentImplInstance == null) {
			try {
				Class managementClassName = forName(usedManagementImplementation);
				Constructor managementConstructor = managementClassName
						.getConstructor(String.class);
				managmentImplInstance = (Management) managementConstructor
						.newInstance(stackName);
				managementImplStore.put(stackName, managmentImplInstance);
			} catch (Exception e) {
			}
		}
		return managmentImplInstance;
	}
}
