#include "..\include\ps\phub.h"
#include "zmq_van.h"
#include <numa.h>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>

PHub::PHub(vector<tuple<NodeId, PHubKey>> keys,
	vector<tuple<PHubKey, NodeId>> dests,
	unordered_map<NodeId, string> nodeToIP,
	PHubRole myRole,
	NodeId id)
{
	myPartitions = keys;
	destinations = dests;
	nodeMap = nodeToIP;
	role = myRole;
	//now connect that.
	ID = id;
}

void PHub::InitializeDevice()
{
	//first, figure out devices.
	machineConfig.SocketCount = numa_available();
	if (machineConfig.SocketCount < 0) machineConfig.SocketCount = 1;
	else machineConfig.SocketCount = numa_num_task_nodes();
	auto maxCpu = getenv("PSHUB_DATA_THREAD_COUNT");
	int allowedCPU = 1;
	if (maxCpu != NULL)
	{
		allowedCPU = atoi(maxCpu);
	}
	CHECK(allowedCPU > 0) << "allowed cpu count is <0";

	//how many processors are there?
	//make sure cpus are balanced.
	CHECK(allowedCPU % machineConfig.SocketCount == 0) << "allowed core count does not divide cpu count";
	machineConfig.CoreCount = allowedCPU;
	auto blockedIFStr = getenv("PHUB_BLOCKED_INTERFACE");
	std::unordered_set<string> blockedIFSet;
	if (blockedIFStr != NULL)
	{
		stringstream ss(blockedIFStr);
		while (ss.good())
		{
			string substr;
			getline(ss, substr, ',');
			blockedIFSet.insert(substr);
			LOG(INFO) << "[" << ID << "] Blocked Interface : " << substr;
		}
	}

	//map a core to a particular socket.
	//do not differentiate worker and server
	auto phubCoreAssignmentScheme = getenv("PHUB_CORE_ASSIGNMENT");
	if (phubCoreAssignmentScheme == NULL)
	{
		phubCoreAssignmentScheme = "sequential";
	}
	machineConfig.Socket2CoreIdx.resize(machineConfig.SocketCount);
	for (int i = 0; i < machineConfig.CoreCount; i++)
	{
		if (strcmp(phubCoreAssignmentScheme, "sequential") == 0)
		{
			machineConfig.Core2SocketIdx.push_back(machineConfig.SocketCount * i / machineConfig.CoreCount);
			machineConfig.Socket2CoreIdx.at(machineConfig.SocketCount * i / machineConfig.CoreCount)++;
		}
		else if (strcmp(phubCoreAssignmentScheme, "roundrobin") == 0)
		{
			machineConfig.Core2SocketIdx.push_back(i %  machineConfig.SocketCount);
			machineConfig.Socket2CoreIdx.at(i %  machineConfig.SocketCount)++;
		}
		else
		{
			CHECK(false) << phubCoreAssignmentScheme << " is not a recognized scheme";
		}
	}
	machineConfig.ib_devices = ibv_get_device_list(&machineConfig.ib_num_devices);
	if (machineConfig.ib_devices != NULL)
	{
		//initialize IB devices only if needed.
		auto phubInterfaceAssignmentScheme = getenv("PHUB_IF_ASSIGNMENT");
		if (phubInterfaceAssignmentScheme == NULL)
		{
			phubInterfaceAssignmentScheme = "sequential";
		}
		CHECK(machineConfig.ib_num_devices <= 10) << "More than 10 devices found.";
		//CHECK(machineConfig.ib_num_devices % machineConfig.SocketCount == 0) << "Unbalanced interface count in different sockets.";
		//machineConfig.ib_Socket2DeviceIdx.resize(machineConfig.SocketCount);

		for (uint32_t i = 0; i < machineConfig.ib_num_devices; i++)
		{
			auto ctx = ibv_open_device(machineConfig.ib_devices[i]);
			auto dev = machineConfig.ib_devices[i];
			string nameStr(ibv_get_device_name(dev));
			CHECK(ctx) << "failed to get context for device " << i << " " << nameStr;
			auto idx = nameStr.back() - '0';
			CHECK(idx >= 0 && idx <= 9) << " this card (" << nameStr << ") has a name that does not conform to the format we understand.";
			auto socketId = 0;
			if (strcmp(phubInterfaceAssignmentScheme, "sequential") == 0)
			{
				socketId = idx * machineConfig.SocketCount / machineConfig.ib_num_devices;
			}
			else if (strcmp(phubInterfaceAssignmentScheme, "roundrobin") == 0)
			{
				socketId = idx % machineConfig.SocketCount;
			}
			else
			{
				CHECK(false) << phubInterfaceAssignmentScheme << " is not a recognized scheme";
			}
			machineConfig.ib_Device2SocketIdx.push_back(socketId);
			ibv_device_attr dev_attr;
			CHECK(ibv_query_device(ctx, &dev_attr)) << " error getting device attributes";
			bool breakDueToDesiredDeviceFound = false;

			for (int p = 1; p < dev_attr.phys_port_cnt; p++)
			{
				
				ibv_port_attr port_attribute;
				CHECK(ibv_query_port(ctx, p, &port_attribute)) << " error getting port attributes";
				MigrateToNumaNode(socketId);
				auto protection_domain = ibv_alloc_pd(ctx);
				CHECK(protection_domain) << " error getting protection domain " << nameStr << " p=" << p;
				if (port_attribute.link_layer != IBV_LINK_LAYER_INFINIBAND || port_attribute.state != IBV_PORT_ACTIVE)
				{
					continue;
				}


			}
		}
	}

} 

int PHub::Push(NodeId destination, BufferHandle buf)
{
	return 0;
}
