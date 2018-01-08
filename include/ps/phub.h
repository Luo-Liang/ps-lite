#pragma once
#include <vector>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <unordered_map>
using namespace std;
typedef uint32_t NodeId;
typedef int BufferHandle;
typedef int PHubKey;


class PHub
{
	struct MachineConfigDescriptor
	{
		bool Initialized = false;
		ibv_device ** ib_devices = NULL;
		int ib_num_devices = 0;
		std::vector<ibv_device*> ib_virtual_devices;
		std::vector< const char *> ib_device_names;
		std::vector<uint64_t> ib_device_guids;
		std::vector<ibv_device_attr> ib_devices_attribute;
		std::vector<uint8_t> ib_ports;
		std::vector<ibv_context *> ib_contexts;
		std::vector<ibv_pd *> ib_protection_domains;
		std::vector<ibv_port_attr> ib_ports_attribute;
		std::vector<int> ib_Device2SocketIdx;
		//std::vector<int> ib_Socket2DeviceIdx;
		int SocketCount = 0;
		int CoreCount = 0;
		std::vector<int> Core2SocketIdx;
		std::vector<int> Socket2CoreIdx;
		std::vector<ibv_mr*> ib_DeviceMemoryRegions;
	};

public:
	enum PHubRole
	{
		Worker,
		Server
	};
	vector<tuple<NodeId, PHubKey>> myPartitions;
	vector<tuple<PHubKey, NodeId>> destinations;
	unordered_map<NodeId, string> nodeMap;
	PHubRole role;
	MachineConfigDescriptor machineConfig;
	//keys that i take care of
	NodeId ID;
	PHub(vector<tuple<NodeId, PHubKey>> keys,
		vector<tuple<PHubKey, NodeId>> dests,
		unordered_map<NodeId,string> nodeToIP,
		PHubRole role,
		NodeId Id);
	void InitializeDevice();
	int Push(NodeId destination, BufferHandle buf);

};