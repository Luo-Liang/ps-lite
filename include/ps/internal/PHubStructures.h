#pragma once
#include "dmlc/logging.h"
#include "dmlc/DIME.h"
#include <ps/internal/van.h>
#include <ps/internal/postoffice.h>
#include <ps/internal/message.h>
#include <sstream>
#include <malloc.h> //memalign
#include "Optimizers.h"
#include <signal.h>
#include <limits.h>
/*#define PHUB_MAX_KEY_BITS 22
#define PHUB_MAX_KEY ((1<<PHUB_MAX_KEY_BITS) - 1)
#define PHUB_IMM_KEY_MASK PHUB_MAX_KEY
#define PHUB_IMM_SENDER_MASK ((1<<(32 - PHUB_MAX_KEY_BITS))-1)*/

//PSHUB requires only 1 sge per message, instead of 2.
//PSHUB requires sending out 2 messages.
//////////////////////////////////////////////////////////////////////////
//For PHUB use only.
struct PHubMergeBuffer
{
public:
	//volatile RemainingReadersForCurrentBuffer; //how many people in the current iteration have pulled?
	volatile int WriteBufferIndex __attribute__((aligned(CACHELINE_SIZE_BYTES))); //either 0 or 1. Buffer used for current iteration.
	//the actual buffer size that's used to align data for sse.
	size_t ActualBufferSizePaddedForSSE;
	size_t ActualElementCountPaddedForSSE;
	//ibv_send_wr ReceiveRequestFastAckSendRequest;
	int Length;
	int FilledLength;
	PLinkKey KeyVal;
	bool initialized;
	int CQIndex = -1;
	bool PSInitialized;//the first push by rank 0 worker
	int QPOffset;

	PHubMergeBuffer()
	{
		initialized = false;
		PSInitialized = false;
	}

	bool Initialized()
	{
		return initialized;
	}
	std::vector<ibv_send_wr> RDMARemoteSendKVRequests;

	std::vector<ibv_sge> RDMAWorkerSendSgesArray;
	void Init(
		int bufferLen,
		PLinkKey key,
		char* pBuf1,
		char* pBuf2,
		std::vector<uint64_t> rdmaRemoteKVBuffers,
		std::vector<int> rKeys,
		NodeId ID,
		int elementWidth)
	{
		CHECK(pBuf1 != NULL);
		CHECK(pBuf2 != NULL);
		Length = bufferLen;
		WriteBufferIndex = 0;
		ActualElementCountPaddedForSSE = RoundUp((bufferLen / elementWidth), INSTRUCTION_VECTOR_SIZE);
		ActualBufferSizePaddedForSSE = ActualElementCountPaddedForSSE * elementWidth;
		Buffer1 = pBuf1;
		memset(Buffer1, 0, ActualBufferSizePaddedForSSE);
		Buffer2 = pBuf2;
		memset(Buffer2, 0, ActualBufferSizePaddedForSSE);

		//The first sizeof(MetaSlim) bytes in buf1 and buf2 are not used in RDMA
		//copy it.
		ibv_send_wr cleanWr;
		ibv_sge cleanSge;
		memset(&cleanSge, 0, sizeof(ibv_sge));
		memset(&cleanWr, 0, sizeof(ibv_send_wr));
		var numRemotes = rdmaRemoteKVBuffers.size();
		RDMARemoteSendKVRequests.resize(numRemotes, cleanWr);
		RDMAWorkerSendSgesArray.resize(numRemotes, cleanSge);

		KeyVal = key;

		for (size_t i = 0; i < numRemotes; i++)
		{
			//SendKVRequests
			RDMARemoteSendKVRequests.at(i).num_sge = 1;
			RDMARemoteSendKVRequests.at(i).sg_list = &RDMAWorkerSendSgesArray[i];
			RDMARemoteSendKVRequests.at(i).opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
			RDMARemoteSendKVRequests.at(i).imm_data = ID | key;
			RDMARemoteSendKVRequests.at(i).wr.rdma.remote_addr = rdmaRemoteKVBuffers.at(i);
			RDMARemoteSendKVRequests.at(i).wr.rdma.rkey = rKeys.at(i);
			//there is no need for metadata.
		}

		CHECK(((uint64_t)Buffer1 & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0) << " requested alignment is " << INSTRUCTION_VECTOR_SIZE * sizeof(float);
		CHECK(((uint64_t)Buffer2 & INSTRUCTION_VECTOR_SIZE_ADDR_MASK) == 0);
		//printf("mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, key);
		ActualElementCountPaddedForSSE = (ActualBufferSizePaddedForSSE) / elementWidth;
		CHECK(KeyVal < PHUB_MAX_KEY);
		initialized = true;

	}

	inline int GetKVBufferLen()
	{
		return Length;
	}

	inline int GetKVBufferLenPaddedForSSE()
	{
		return ActualBufferSizePaddedForSSE;
	}

	inline int GetKVElementCountAccountingForSSEPadding()
	{
		return ActualElementCountPaddedForSSE;
	}
	//The buffer you use to perform aggregation.
	inline char* GetCurrentGradMergeBuffer()
	{
		//write buffer is always opposite from pull buffer.
		auto ret = WriteBufferIndex == 0 ? Buffer1 : Buffer2;
		//printf("[W]mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, KeyVal);

		return ret;
	}
	//The model buffer
	inline char* GetCurrentWeightBuffer()
	{
		CHECK(PSInitialized);
		auto ret = WriteBufferIndex == 0 ? Buffer2 : Buffer1;
		//printf("[r]mbuffer b1 = %p, b2 = %p, k = %d\n", Buffer1, Buffer2, KeyVal);

		return ret;
	}

	void FlipBuffer()
	{
		WriteBufferIndex = (WriteBufferIndex + 1) & 1;
	}


	char* Buffer1;
	char* Buffer2;
};

struct IBReceiveRequest
{
	size_t QPIndex;// __attribute__((aligned(CACHELINE_SIZE_BYTES)));
	ibv_recv_wr ReceiveRequest;
	//IBReceiveRequest(size_t qpIndex, size_t id)
	//{
	//	QPIndex = qpIndex;
	//	memset(&ReceiveRequest, 0, sizeof(ibv_recv_wr));
	//	ReceiveRequest.wr_id = id;
	//}
};
