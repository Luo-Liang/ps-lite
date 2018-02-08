#pragma once
#include <unordered_map>
#include <malloc.h>
#include "consts.h"
#include <vector>
#include <numa.h>
#include "Helpers.h"
//allocator speaks VIRTUAL key

template <class T>
class PHubAllocator
{
public:
	size_t KeyCount;

	//Note that this aliases with PHUBReceiveKVBuffer
	void* PHUBSendKVBuffer(int key, int copyIdx, int socketId, size_t& outLength)
	{
		return PHUBReceiveKVBuffer(key, copyIdx, socketId, outLength);
	}

	void* PHUBMergeKVBuffer(int key, int socketId, int copyIdx, size_t& outLength)
	{
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

	void* PHUBReceiveMetaBuffer(int key, int copyIdx, int socketId, size_t& outLength)
	{
		CHECK(PHUBRecvMeta[socketId][copyIdx][key].first != NULL);
		outLength = PHUBRecvMeta[socketId][copyIdx][key].second;
		return (void*)PHUBRecvMeta[socketId][copyIdx][key].first;
	}

	void* PHUBMergeMetaBuffer(int key, int copyIdx, int socketId, size_t& outLength)
	{
		CHECK(PHUBMergeMeta[socketId][copyIdx][key].first != NULL);
		outLength = PHUBMergeMeta[socketId][copyIdx][key].second;
		return (void*)PHUBMergeMeta[socketId][copyIdx][key].first;
	}
	//alias to receive metabuffer.
	void* PHUBSendMetaBuffer(int key, int copyIdx, int socketId, size_t& outLength)
	{
		return PHUBReceiveMetaBuffer(key, copyIdx, socketId, outLength);
	}

	void GetAllocatedRange(void*& start, int socketId, int& length)
	{
		start = StartAddresses.at(socketId);
		length = AllocationLength;
	}

	bool IsInitialized()
	{
		return Initialized;
	}
	//this needs to be in multiple stages because kvstore_dist does not have access to verb but is required to allocate buffer on the worker side.
	//if requires merge buffer is toggled, it is a PHUB.
	//a PHUB requires 2 merge buffers per key (for aggregating and reading).
	//if not toggled, it is a infiniband van.
	//an infiniband van requires only 1 buffer per key, because send and receive of a key is never overlapped.
	void Init(std::vector<float>& keySizes,
		//bool RequiresMergeBuffer,
		int copies,
		int metaBufferSize,
		std::vector<int> key2Socket,
		//this one better be false for Gloo to work efficiently.
		bool metaBufferImmediatelyBeforeKVBuffer)
	{
		//keep the keySizes.
		keyMap = keySizes;
		MetaBufferImmediatelyBeforeKVBuffer = metaBufferImmediatelyBeforeKVBuffer;
		int metaSize = metaBufferSize;
		//allow workers to have multiple sockets.
			//if (ps::Postoffice::Get()->van()->my_node().role == ps::Node::WORKER)
			//{
		//CHECK(socketCnt == 1);
		//}
		CHECK(!Initialized);
		KeyCount = keySizes.size();
		socketCount = *(max_element(key2Socket.begin(), key2Socket.end())) + 1;



		//need to account for metaslim sizes.
		//Consider theresizeo are 3 things:
		//Send Buffer (for worker).
		//Receive Buffer.
		//Merge Buffer
		//each of them need a series of MetaSlim
		//how many bytes do we need?
		uint64_t bytes = 0;
		std::vector<size_t> paddedActualKeySizes(keySizes.size());
		for (size_t i = 0; i < keySizes.size(); i++)
		{
			//metadata * 3
			//use padding.
			size_t paddedElementCount = RoundUp(keySizes[i] / sizeof(float), INSTRUCTION_VECTOR_SIZE);
			size_t paddedSize = paddedElementCount * sizeof(float);
			bytes += paddedSize;
			paddedActualKeySizes[i] = paddedSize;
			//printf("k = %d, orig size = %d, padded size = %d\n", i, keySizes[i], paddedSize);
		}





		uint64_t totalBytes = 0;
		//3 * numberofkeys * sizeof(metaslim).
		auto kvCount = 1;// + (RequiresMergeBuffer == true ? 2 : 0);
		//receive buffer + 2 merge buffers OR receive buffer + send buffer.
		//bytes needed for kv
		totalBytes += kvCount * bytes;
		//bytes needed for meta
		//for PHUB, requires meta for receive (and send), and two merge buffers (one of them is not necessary) per key.
		// + (RequiresMergeBuffer == true ? 2 : 0);
		totalBytes += keySizes.size() * metaSize;

		//now multiply that by the number of copies requested.
		totalBytes *= copies;

		//if a pshub, we need to add 2 copies of merge buffer and 2 copies of merge buffer meta per key.

		totalBytes += 2 * (bytes + metaSize * keySizes.size());
		StartAddresses.resize(socketCnt);
		PHUBRecvKV.resize(socketCnt);
		PHUBRecvMeta.resize(socketCnt);
		//one copy of merge buffer required.
		PHUBMergeKV.resize(socketCnt);
		PHUBMergeMeta.resize(socketCnt);
		// we need this amount of data per socke
		StartAddresses.resize(socketCnt);

		for (int socketId = 0; socketId < socketCnt; socketId++)
		{
			auto addr = AlignedAllocateUniversal(totalBytes, socketId, INSTRUCTION_VECTOR_SIZE * sizeof(float));
			StartAddresses.at(socketId) = addr;
			CHECK(addr) << " Requesting to allcoate " << totalBytes / 1024.0 / 1024.0 / 1024.0 << "GB of data failed";
			//ugly
			CHECK((metaSize & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0u);
			AllocationLength = totalBytes;

			PHUBRecvKV.at(socketId).resize(copies);
			PHUBRecvMeta.at(socketId).resize(copies);
			//one copy of merge buffer required.
			PHUBMergeKV.at(socketId).resize(2);
			PHUBMergeMeta.at(socketId).resize(2);

			for (int i = 0; i < copies; i++)
			{
				PHUBRecvKV.at(socketId).at(i).resize(keySizes.size());
				PHUBRecvMeta.at(socketId).at(i).resize(keySizes.size());
			}
			PHUBMergeKV.at(socketId)[0].resize(keySizes.size());
			PHUBMergeMeta.at(socketId)[0].resize(keySizes.size());
			//now assign values!
			void* cursor = addr;
			//deal with SendMeta.
			for (int cp = 0; cp < copies; cp++)
			{
				for (size_t i = 0; i < keySizes.size(); i++)
				{
					//now setup receive buffer meta.
					PHUBRecvMeta.at(socketId)[cp][i].first = cursor;
					PHUBRecvMeta.at(socketId)[cp][i].second = metaSize;
					cursor = cursor + metaSize;
					//now setup receive kv buffer.
					CHECK(((uint64_t)cursor & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
					PHUBRecvKV.at(socketId)[cp][i].first = cursor;
					PHUBRecvKV.at(socketId)[cp][i].second = paddedActualKeySizes[i];
					cursor = cursor + PHUBRecvKV.at(socketId)[cp][i].second;
					if (cp == 0)
					{
						//i need two merge buffers.
						PHUBMergeMeta.at(socketId)[cp][i].first = cursor;
						PHUBMergeMeta.at(socketId)[cp][i].second = metaSize;
						cursor = cursor + metaSize;

						//setup merge kv buffer 1
						CHECK(((uint64_t)cursor & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
						PHUBMergeKV.at(socketId).at(cp).at(i).first = cursor;
						PHUBMergeKV.at(socketId).at(cp).at(i).second = paddedActualKeySizes[i];
						cursor = cursor + PHUBMergeKV.at(socketId)[cp][i].second;
					}
				}
			}
			memset(StartAddresses.at(socketId), 0, AllocationLength);
			if (StartAddresses.at(socketId) + AllocationLength != cursor)
			{
				raise(SIGTRAP);
			}
		}
		CHECK(AllocationLength != 0) << " socketCnt = " << socketCount << " machineCount = " << copies;

		//CHECK(StartAddress + AllocationLength == cursor);
		Initialized = true;
	}
	void* GetStartAddress(int socketIdx, size_t& len)
	{
		CHECK(Initialized);
		CHECK(AllocationLength != 0);
		len = AllocationLength;
		return StartAddresses.at(socketIdx);
	}
	bool MetaKVContinuous()
	{
		return MetaBufferImmediatelyBeforeKVBuffer;
	}

	void VerifyPHUBRecvKV(int socketId, int remoteRank, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBRecvKV.at(socketId).at(remoteRank).at(key).first == start);
		CHECK(PHUBRecvKV.at(socketId).at(remoteRank).at(key).second == (size_t)endExclusive - (size_t)start);
	}

	void VerifyPHUBRecvMeta(int socketId, int remoteRank, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBRecvMeta.at(socketId).at(remoteRank).at(key).first == start);
		CHECK(PHUBRecvMeta.at(socketId).at(remoteRank).at(key).second == (size_t)endExclusive - (size_t)start);
	}

	void VerifyPHUBMergeKV(int socketId, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBMergeKV.at(socketId).at(0).at(key).first == start);
		CHECK(PHUBMergeKV.at(socketId).at(0).at(key).second == (size_t)endExclusive - (size_t)start);
	}

	void VerifyPHUBMergeMeta(int socketId, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBMergeMeta.at(socketId).at(0).at(key).first == start);
		CHECK(PHUBMergeMeta.at(socketId).at(0).at(key).second == (size_t)endExclusive - (size_t)start);
	}
	void VerifyPHUBMergeKV1(int socketId, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBMergeKV.at(socketId).at(1).at(key).first == start);
		CHECK(PHUBMergeKV.at(socketId).at(1).at(key).second == (size_t)endExclusive - (size_t)start);
	}

	void VerifyPHUBMergeMeta1(int socketId, int key, void* start, void* endExclusive)
	{
		CHECK(PHUBMergeMeta.at(socketId).at(1).at(key).first == start);
		CHECK(PHUBMergeMeta.at(socketId).at(1).at(key).second == (size_t)endExclusive - (size_t)start);
	}
private:
	bool MetaBufferImmediatelyBeforeKVBuffer;
	//socket id -> copy id->key 
	std::vector<std::vector<std::vector<std::pair<void*, size_t>>>> PHUBRecvKV;
	std::vector<std::vector<std::vector<std::pair<void*, size_t>>>> PHUBRecvMeta;
	std::vector<std::vector<std::vector<std::pair<void*, size_t>>>> PHUBMergeKV;
	std::vector<std::vector<std::vector<std::pair<void*, size_t>>>> PHUBMergeMeta;
	std::vector<void*> StartAddresses;
	size_t AllocationLength = 0;
	bool Initialized = false;
	size_t socketCount = 0;
	vector<float> keyMap;
};
