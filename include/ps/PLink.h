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
#include "internal/PHubStructures.h"
#include "phub.h"
#include "../dmlc/logging.h"
#include <thread>
using namespace std;
typedef uint32_t NodeId;
typedef uint64_t BufferHandle;
typedef uint32_t PLinkKey;

#define ToBufferHandle(nid, handle) ((BufferHandle)(nid << 32 | handle))
#define NodeIdFromHandle(handle) ((NodeId)(handle >> 32))
#define KeyFromHandle(handle) ((PLinkKey)(handle & 0xFFFFFFFF))
struct PLinkWorkQueue
{
	//only one pending operation per key or layer is needed
	//a thread simply scans the keys it is in charge of.
	int KeyCount;
	PLinkWorkQueue(int keyCount)
	{
		KeyCount = keyCount;
		WorkQueues.resize(keyCount);
	}
	vector<shared_ptr<ScheduleNode>> WorkQueues;
};

class PLinkExecutor
{
	//a phub
	//a set of gloo algorithms
	//a workqueue
	shared_ptr<PLinkWorkQueue> wQs;
	shared_ptr<PHub> pHub;
	vector<thread> threads;
	void Execute(int tid);
	volatile bool gtg = false;
	void ReadiyGraph();
	unordered_map<PLinkKey, Schedule> perKeySchedule;
	unordered_map<PLinkKey, vector<ScheduleNode>> currentNodePerKeySchedule;
public:
	void Initialize(unordered_map<PLinkKey, Schedule> schedules,
		string redezvousUri,
		unordered_map<NodeId, string> nodeToIP,
		vector<float>& sizes,
		vector<void*> applicationSuppliedAddrs,
		int totalParticipant,
		int elementWidth,
		NodeId Id);
	void GTG()
	{
		gtg = true;
	}
};