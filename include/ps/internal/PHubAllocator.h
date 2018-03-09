#pragma once
#include <unordered_map>
#include <malloc.h>
#include "consts.h"
#include <vector>
#include <numa.h>
#include "Helpers.h"
//allocator speaks VIRTUAL key

class PHubAllocator
{
public:
	size_t KeyCount;

	void* PHUBMergeKVBuffer(int key, int socketId, int copyIdx, size_t& outLength)
	{
		CHECK(copyIdx <= 1 && copyIdx >= 0);
		CHECK(PHUBMergeKV[socketId][copyIdx][key].first != NULL);
		outLength = PHUBMergeKV[socketId][copyIdx][key].second;
		return PHUBMergeKV[socketId][copyIdx][key].first;
	}

	void* PHUBReceiveKVBuffer(int key, int copyIdx, int socketId, size_t& outLength)
	{
		CHECK(PHUBRecvKV[socketId][copyIdx][key].first != NULL);
		outLength = PHUBRecvKV[socketId][copyIdx][key].second;
		return PHUBRecvKV[socketId][copyIdx][key].first;
	}

	void GetAllocatedRange(int socketId, void*& start, int& length)
	{
		start = StartAddresses.at(socketId);
		length = perSocketBytes.at(socketId);
	}

	bool IsInitialized()
	{
		return Initialized;
	}
	//a phub requires the following per key:
	// per machine receive buffer
	// an aggregation buffer
	// a read buffer.

	//sizes in BYTES
	void Init(std::vector<float>& sizes,
		//bool RequiresMergeBuffer,
		int copies,
		//int metaBufferSize,
		std::vector<int> k2Socket,
		int elementWidth = 4)
	{
		//keep the keySizes.
		keySizes = sizes;
		CHECK(!Initialized);
		KeyCount = keySizes.size();
		socketCount = *(max_element(key2Socket.begin(), key2Socket.end())) + 1;
		key2Socket = k2Socket;
		key2InSocketIdx.resize(keySizes.size());

		vector<int> socketTicketers(socketCount);
		vector<vector<int>> socket2Keys(socketCount);

		perSocketBytes.resize(socketCount);
		std::vector<size_t> paddedActualKeySizes(keySizes.size());
		for (Cntr i = 0; i < sizes.size(); i++)
		{
			var sock = key2Socket.at(i);
			var& ticket = socketTicketers.at(sock);
			key2InSocketIdx.at(i) = ticket;
			socket2Keys.at(sock).push_back(i);
			ticket++;

			size_t paddedElementCount = RoundUp(keySizes[i] / elementWidth, INSTRUCTION_VECTOR_SIZE);
			size_t paddedSize = paddedElementCount * elementWidth;
			perSocketBytes.at(sock) += paddedSize;
			paddedActualKeySizes.at(i) = paddedSize;
		}

		StartAddresses.resize(socketCount);
		PHUBRecvKV.resize(socketCount);
		//one copy of merge buffer required.
		PHUBMergeKV.resize(socketCount);
		// we need this amount of data per socke

		for (int socketId = 0; socketId < socketCount; socketId++)
		{
			var totalBytes = perSocketBytes.at(socketId);
			auto addr = AlignedAllocateUniversal(totalBytes, socketId, INSTRUCTION_VECTOR_SIZE * elementWidth);
			StartAddresses.at(socketId) = addr;
			CHECK(addr) << " Requesting to allcoate " << totalBytes / 1024.0 / 1024.0 / 1024.0 << "GB of data failed";
			//ugly
			PHUBRecvKV.at(socketId).resize(copies);
			//one copy of merge buffer required.
			PHUBMergeKV.at(socketId).resize(2);
			var sockKeyCnt = socket2Keys.at(socketId).size();

			for (int i = 0; i < copies; i++)
			{
				PHUBRecvKV.at(socketId).at(i).resize(sizes.size());
			}
			//set up traps.
			PHUBMergeKV.at(socketId)[0].resize(sizes.size());
			PHUBMergeKV.at(socketId)[1].resize(sizes.size());

			//now assign values!
			void* cursor = addr;
			for (int cp = 0; cp < copies; cp++)
			{
				for (size_t i = 0; i < sockKeyCnt; i++)
				{
					var realKey = socket2Keys.at(socketId).at(i);
					//now setup receive kv buffer.
					CHECK(((uint64_t)cursor & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
					PHUBRecvKV.at(socketId)[cp][realKey].first = cursor;
					PHUBRecvKV.at(socketId)[cp][realKey].second = paddedActualKeySizes[realKey];
					cursor = cursor + PHUBRecvKV.at(socketId)[cp][realKey].second;
					if (cp <= 1)
					{
						//setup merge kv buffer 0 and 1
						CHECK(((uint64_t)cursor & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
						PHUBMergeKV.at(socketId).at(cp).at(realKey).first = cursor;
						PHUBMergeKV.at(socketId).at(cp).at(realKey).second = paddedActualKeySizes[realKey];
						cursor = cursor + PHUBMergeKV.at(socketId)[cp][realKey].second;
					}
				}
			}
			memset(StartAddresses.at(socketId), 0, perSocketBytes.at(socketId));
			if (StartAddresses.at(socketId) + perSocketBytes.at(socketId) != cursor)
			{
				raise(SIGTRAP);
			}
		}
		//CHECK(StartAddress + AllocationLength == cursor);
		//register mbrs.

		Initialized = true;
	}

	vector<ibv_mr*> ReadyRDMA(vector<ibv_pd*>& pPds, vector<int>& pd2Sock)
	{
		CHECK(Initialized);
		if (MemoryRegions.size() == 0)
		{
			for (Cntr i = 0; i < pPds.size(); i++)
			{
				var pd = pPds.at(i);
				var sock = pd2Sock.at(i);
				var startAddr = StartAddresses.at(sock);
				var sz = perSocketBytes.at(sock);
				ibv_mr * mr = ibv_reg_mr(pd,
					startAddr, sz,
					(IBV_ACCESS_LOCAL_WRITE |
						IBV_ACCESS_REMOTE_WRITE |
						IBV_ACCESS_REMOTE_READ |
						IBV_ACCESS_REMOTE_ATOMIC));
				CHECK(mr != NULL);
				MemoryRegions.push_back(mr);
			}
		}
		return MemoryRegions;
	}

	void* GetStartAddress(int socketIdx, size_t& len)
	{
		CHECK(Initialized);
		return StartAddresses.at(socketIdx);
	}

	void VerifyPHUBRecvKV(int socketId, int remoteRank, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBRecvKV.at(socketId).at(remoteRank).at(key).first == start);
		CHECK(PHUBRecvKV.at(socketId).at(remoteRank).at(key).second == (size_t)endExclusive - (size_t)start);
	}

	void VerifyPHUBMergeKV(int socketId, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBMergeKV.at(socketId).at(0).at(key).first == start);
		CHECK(PHUBMergeKV.at(socketId).at(0).at(key).second == (size_t)endExclusive - (size_t)start);
	}
	//socket to ibv_mr
	vector<ibv_mr*> MemoryRegions;

private:
	//socket id -> copy id->key 
	std::vector<std::vector<std::vector<std::pair<void*, size_t>>>> PHUBRecvKV;
	//socket id->copy id -> key
	std::vector<std::vector<std::vector<std::pair<void*, size_t>>>> PHUBMergeKV;
	std::vector<void*> StartAddresses;
	bool Initialized = false;
	size_t socketCount = 0;
	vector<float> keySizes;
	vector<int> key2Socket;
	//the ith key in the selected index.
	vector<int> key2InSocketIdx;
	std::vector<size_t> perSocketBytes;
};
