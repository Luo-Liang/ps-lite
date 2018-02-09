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
#include "internal/ext.h"
#include "rendezvous.h"
#include "internal/PHubAllocator.h"

using namespace std;
typedef uint32_t NodeId;
typedef uint64_t BufferHandle;
typedef uint32_t PLinkKey;

#define ToBufferHandle(nid, handle) ((BufferHandle)(nid << 32 | handle))
#define NodeIdFromHandle(handle) ((NodeId)(handle >> 32))
#define KeyFromHandle(handle) ((PLinkKey)(handle & 0xFFFFFFFF))

#define MAX_PHUB_NODES 1000

struct KeyDesc
{
	PLinkKey Key;
	EndPoint EP;
};
//represent one connection of two infiniband or IP nodes
struct EndPoint
{
	uint16_t lid = -1;        // InfiniBand address of node. Remote Lid
	uint32_t qp_num = -1;     // Queue pair number on node (like IP port number)
	ibv_qp * queue_pair = NULL;
	int DeviceIdx = -1;
	int SocketIdx = -1;
	int RemoteMachineIdx = -1;
	int CoreIdx = -1;
	int RemoteQPIdx = -1;
	int CQIdx = -1;
	int Index = -1;
	uint32_t RemoteKey = 0;
	uint16_t LocalLid = 0;
};

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
	std::vector<int> Socket2CoreCount;
	std::vector<ibv_mr*> ib_DeviceMemoryRegions;
	std::vector<vector<int>> Socket2Core;


};

struct MachineConfigDescSlim
{
public:
	std::vector<int> Devices2Socket;
	std::vector<int> Cores2Socket;
	int NumSockets;
};

struct PHubEndpoint {
	uint16_t LocalLid = -1;
	uint16_t RemoteLid = -1;        // InfiniBand address of node. Remote Lid
	uint32_t RemoteQPNum = -1;     // Queue pair number on node (like IP port number)
	uint32_t LocalQPNum = -1;
	int DeviceIdx = -1;
	int SocketIdx = -1;
	int RemoteMachineIdx = -1;
	int RemoteCardIdx = -1;
	int CoreIdx = -1;
	int CQIdx = -1;
	int Index = -1;
};

class PHub
{

	Schedule schedule;
	vector<NodeId> remotes;
	shared_ptr<vector<KeyDesc>> pKeyDescs;
	std::vector<ibv_qp*> QPs;
	std::vector<ibv_cq*> SCQs;
	std::vector<ibv_cq*> RCQs;
	int totalPHubNodes;
	std::vector<PHubEndpoint> Endpoints;
	unordered_map<PLinkKey, unordered_map<NodeId, int>> KeyToQPIdx;
	PHubAllocator allocator;
	int ElementWidth;
public:
	//global keysizes assuming contiguous keys.
	//in bytes;
	vector<float> keySizes;
	unordered_map<NodeId, string> nodeMap;
	MachineConfigDescriptor machineConfig;
	//my global ID
	NodeId ID;
	PHub(Schedule schedule, string redezvousUri,
		unordered_map<NodeId, string> nodeToIP,
		vector<float>& sizes,
		int totalParticipant,
		int elementWidth,
		NodeId Id);
	string RendezvousUri;

	shared_ptr<Rendezvous> phubRendezvous = NULL;
	shared_ptr<gloo::rendezvous::RedisStore> pRedisStore = NULL;
	shared_ptr<gloo::transport::Device> pGlooDefaultDevice = NULL;
	void InitializeDevice();
	int Push(NodeId destination, BufferHandle buf);
	void InitializeDeviceSpecifics();
	void InitializePHubSpecifics();
	void Push(PLinkKey pkey, NodeId target);
	void Pull(PLinkKey pkey, NodeId source);
	void Broadcast(PLinkKey pkey, vector<NodeId>& targets);
	void Gather(PLinkKey pkey, vector<NodeId>& sources);
	shared_ptr<vector<KeyDesc>> RetrieveKeyDescs()
	{
		return pKeyDescs;
	}
};