#pragma once
#include <vector>
#include <infiniband/arch.h>
#include <infiniband/verbs.h>
#include <unordered_map>
#include "Schedule.h"
#include "../dmlc/logging.h"
#include <gloo/context.h>
#include <gloo/allreduce_halving_doubling.h>
#include <gloo/allreduce_ring_chunked.h>
#include <gloo/transport/ibverbs/device.h>
#include <gloo/rendezvous/context.h>
#include <gloo/rendezvous/redis_store.h>
#include <gloo/algorithm.h>
using namespace std;
typedef uint32_t NodeId;
typedef uint64_t BufferHandle;
typedef uint32_t PHubKey;

#define ToHandle(nid, Handle) ((BufferHandle)(nid << 32 | Handle))
#define NodeIdFromHandle(Handle) ((NodeId)(Handle >> 32))
#define KeyFromHandle(Handle) ((PHubKey)(Handle & 0xFFFFFFFF))

#define MAX_PHUB_NODES 1000

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
	Schedule schedule;
	vector<NodeId> remotes;

	void initializeGlooSpecifics();
public:
	unordered_map<NodeId, string> nodeMap;
	MachineConfigDescriptor machineConfig;
	//keys that i take care of
	NodeId ID;
	PHub(Schedule schedule,
		unordered_map<NodeId,string> nodeToIP,
		NodeId Id);
	void InitializeDevice();
	int Push(NodeId destination, BufferHandle buf);
	void InitializeDeviceSpecifics();
};