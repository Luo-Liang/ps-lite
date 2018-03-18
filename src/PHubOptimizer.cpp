#pragma once
#include <stdlib.h>
#include <string>
#include <vector>
#include <unordered_map>
#include <xmmintrin.h>
#include <malloc.h>
#include <string.h>
#include <cassert>
#include <ps/PHubOptimizer.h>
#ifndef __INTELLISENSE__
#include <signal.h>
#include "consts.h"
#endif

class Optimizer
{
public:
	//name of optimizer, number of keys to be optimized, number of machines
	void PopulateTrainingParams(float learningRate, float wd, float mom, float gradScale)
	{
		//CHECK(NegationOfLearningRates.size() == size);
		//CHECK(WeightDecays.size() == size);
		NegationOfLearningRate = _mm_set1_ps(-1 * learningRate);
		WeightDecay = _mm_set1_ps(wd);
		MomentumConstant = _mm_set1_ps(mom);
		GradRescaling = _mm_set1_ps(gradScale);
		printf("[note][PHUB] setting mom = %f, gradrescale = %f and learning rate and weight decays.\n", mom, learningRate]);
	}
	virtual void Update(PLinkKey key, float* weightBuffer, float* gradBuffer, size_t bufferLen) {}
	Optimizer(size_t numMachines, PLinkKey key, size_t len, const size_t prefetch_distance, int sockId)
		: prefetch_distance(prefetch_distance)
	{
		Key = key;
		MachineCount = numMachines;
		//populate momentumconstant, learningrates, and weightdecays, gradrescaling.
		float rescale = 1.0f / 128; // 1 / batch size
		GradRescaling = _mm_set1_ps(rescale);

		//weight decays. this one we need to copy the orig from mxnet, but assign 0.00001 at first.
		auto initWd = 1.0e-5f;
		//work on only 1 keys.
		WeightDecay = _mm_set1_ps(initWd);
		//learning rates.
		auto initLR = 0.1f;
		NegationOfLearningRate = _mm_set1_ps(-1 * initLR);
		//momentum constants
		MomentumConstant = _mm_set1_ps(0.9f);

		//initialize current states.
		//make this a multiple of INSTRUCTION_VECTOR_SIZE so we dont need to deal with remainder.
		auto bytes = RoundUp((size_t)len, INSTRUCTION_VECTOR_SIZE) * sizeof(float);
		//if (verbs == NULL || verbs->DirectConnect || verbs->Helper_Server_IsItMyKey(i) == false)
		//{
		//	//verbs may be NULL if Gloo
		//	//if this key is not mine, i still create a buffer for it.
		//	//just that i don't care which node it is on.
		//	state[i] = (float*)memalign(INSTRUCTION_VECTOR_SIZE * sizeof(float), bytes);
		//}
		//else
		{
			//NUMA Aware.
			auto mySock = sockId;
			state = (float*)AlignedAllocateUniversal(bytes, mySock);//default 2MB alignment. 
			CHECK(state != NULL) << "Cannot allocate state buffer on numa node " << mySock << ". Too many allocations?";
		}

		StateLength = bytes;
		memset(state, 0, bytes);
	}

	virtual ~Optimizer() { }

	__m128 WeightDecay; //weight decay
	__m128 NegationOfLearningRate;
	float* state; // key -> expanded vectors of momentum.
	size_t StateLength;
	__m128 MomentumConstant;//the expanded momentum vectors.
	__m128 GradRescaling;
	size_t NumberOfUpdates = 0;
	size_t MachineCount;
	std::string Name;
	size_t OptimizationCounts;
	//clipping, lr scheduler, idx2Name ignored.
	PLinkKey Key;
	const size_t prefetch_distance;
};

class NAGTTOptimizer : public Optimizer
{
public:
	NAGTTOptimizer(size_t numMachines, PLinkKey key, size_t len, const size_t prefetch_distance, int socketId)
		: Optimizer(numMachines, key, len, prefetch_distance, socketId)
	{
		this->Name = "nagTT";
	}

	//index, weights and gradients (aggregated) 
	//weight: old, last iteration weight buffer.
	//gradbuffer: new, aggregated buffer.
	virtual void Update(PLinkKey key, float* weightBuffer, float* gradBuffer, size_t bufferLen) override
	{
		//sanity check
		CHECK(key == Key);
		size_t offset = 0;
		size_t prefetch_offset = prefetch_distance;

		float * momBuffer = state;

		__asm__ __volatile__(/* last_update = update; */
							  /* update = momentum * update - learning_rate * deltas; */
							  /* thetas += -momentum * last_update + (1 + momentum) * update */

			".align 32 \n\t"
			"1: \n\t"

			// grad = grad * self.rescale_grad
			// ----
			// load grad
			"movaps         0x00(%[grad],%[offset],1),%%xmm0 \n\t"
			"movaps         0x10(%[grad],%[offset],1),%%xmm1 \n\t"
			"movaps         0x20(%[grad],%[offset],1),%%xmm2 \n\t"
			"movaps         0x30(%[grad],%[offset],1),%%xmm3 \n\t"
			// mul grad scalar scaling_factor
			"mulps          %[scaling_factor],%%xmm0 \n\t"
			"mulps          %[scaling_factor],%%xmm1 \n\t"
			"mulps          %[scaling_factor],%%xmm2 \n\t"
			"mulps          %[scaling_factor],%%xmm3 \n\t"

			// mom[:] *= self.momentum
			// ----
			// load mom
			"movaps         0x00(%[mom],%[offset],1),%%xmm4 \n\t"
			"movaps         0x10(%[mom],%[offset],1),%%xmm5 \n\t"
			"movaps         0x20(%[mom],%[offset],1),%%xmm6 \n\t"
			"movaps         0x30(%[mom],%[offset],1),%%xmm7 \n\t"
			// mul mom scalar momentum_factor
			"mulps          %[momentum_factor],%%xmm4 \n\t"
			"mulps          %[momentum_factor],%%xmm5 \n\t"
			"mulps          %[momentum_factor],%%xmm6 \n\t"
			"mulps          %[momentum_factor],%%xmm7 \n\t"

			// grad += wd * weight
			// ----
			// load weight
			"movaps         0x00(%[weight],%[offset],1),%%xmm8 \n\t"
			"movaps         0x10(%[weight],%[offset],1),%%xmm9 \n\t"
			"movaps         0x20(%[weight],%[offset],1),%%xmm10 \n\t"
			"movaps         0x30(%[weight],%[offset],1),%%xmm11 \n\t"
			// make copy for weight decay calculation
			"movaps          %%xmm8,%%xmm12 \n\t"
			"movaps          %%xmm9,%%xmm13 \n\t"
			"movaps          %%xmm10,%%xmm14 \n\t"
			"movaps          %%xmm11,%%xmm15 \n\t"
			// mul weight scalar weight_decay_factor
			"mulps          %[weight_decay_factor],%%xmm12 \n\t"
			"mulps          %[weight_decay_factor],%%xmm13 \n\t"
			"mulps          %[weight_decay_factor],%%xmm14 \n\t"
			"mulps          %[weight_decay_factor],%%xmm15 \n\t"
			// add grad weight
			"addps          %%xmm12,%%xmm0 \n\t"
			"addps          %%xmm13,%%xmm1 \n\t"
			"addps          %%xmm14,%%xmm2 \n\t"
			"addps          %%xmm15,%%xmm3 \n\t"

			// mom[:] += grad
			// ----
			// add mom grad
			"addps          %%xmm0,%%xmm4 \n\t"
			"addps          %%xmm1,%%xmm5 \n\t"
			"addps          %%xmm2,%%xmm6 \n\t"
			"addps          %%xmm3,%%xmm7 \n\t"
			// store mom
			"movaps %%xmm4,0x00(%[mom],%[offset],1) \n\t"
			"movaps %%xmm5,0x10(%[mom],%[offset],1) \n\t"
			"movaps %%xmm6,0x20(%[mom],%[offset],1) \n\t"
			"movaps %%xmm7,0x30(%[mom],%[offset],1) \n\t"

			// grad[:] += self.momentum * mom
			// ----
			// mul mom scalar momentum_factor
			"mulps          %[momentum_factor],%%xmm4 \n\t"
			"mulps          %[momentum_factor],%%xmm5 \n\t"
			"mulps          %[momentum_factor],%%xmm6 \n\t"
			"mulps          %[momentum_factor],%%xmm7 \n\t"
			// add grad mom
			"addps          %%xmm4,%%xmm0 \n\t"
			"addps          %%xmm5,%%xmm1 \n\t"
			"addps          %%xmm6,%%xmm2 \n\t"
			"addps          %%xmm7,%%xmm3 \n\t"

			// weight[:] += -lr * grad
			// ----
			// mul grad scalar learning_rate
			"mulps          %[learning_rate],%%xmm0 \n\t"
			"mulps          %[learning_rate],%%xmm1 \n\t"
			"mulps          %[learning_rate],%%xmm2 \n\t"
			"mulps          %[learning_rate],%%xmm3 \n\t"
			// sub weight grad (already negative)
			"addps          %%xmm0,%%xmm8 \n\t"
			"addps          %%xmm1,%%xmm9 \n\t"
			"addps          %%xmm2,%%xmm10 \n\t"
			"addps          %%xmm3,%%xmm11 \n\t"
			// store weight
			"movaps  %%xmm8,0x00(%[weight],%[offset],1) \n\t"
			"movaps  %%xmm9,0x10(%[weight],%[offset],1) \n\t"
			"movaps %%xmm10,0x20(%[weight],%[offset],1) \n\t"
			"movaps %%xmm11,0x30(%[weight],%[offset],1) \n\t"

			"add %[inc],%[offset] \n\t"
			"cmp %[end],%[offset] \n\t"
			"jb 1b \n\t"

			"4:"
			"sfence \n\t"

			: [offset] "+r" (offset)//,
			  //[prefetch_offset] "+r" (prefetch_offset)
			: [grad]   "r" (gradBuffer),
			[mom]    "r" (momBuffer),
			[weight] "r" (weightBuffer),

			[learning_rate] "m" (NegationOfLearningRate),
			[momentum_factor] "m" (MomentumConstant),
			[scaling_factor] "m" (GradRescaling),
			[weight_decay_factor] "m" (WeightDecay),

			[inc] "i" (CACHELINE_SIZE_BYTES),

			[end] "mr" (bufferLen * sizeof(float))

			: "cc", "memory",
			"xmm0", "xmm1", "xmm2", "xmm3",
			"xmm4", "xmm5", "xmm6", "xmm7",
			"xmm8", "xmm9", "xmm10", "xmm11",
			"xmm12", "xmm13", "xmm14", "xmm15");

	}
};



void PHubOptimizerOp::Initialize(shared_ptr<OperatorContext> context)
{
	Type = OperatorType::PHubOptimizer;
	CHECK(context->typeCode == OperatorContext::LocallyAvailable);
	CHECK(numAggregated > 0);
	//CHECK(pKeyDescs != NULL);
	//extract phub context.
	pHub = dynamic_pointer_cast<PHub>(context->additionalContext);
	var sz = pHub->keySizes.at(key);

	//create keyDescs.
	var socketId = pHub->GetSocketAffinityFromKey(key);
	PHubAllocator& allocator = pHub->GetAllocator();
	int notUsed;
	var mBuffer = pHub->RetrieveMergeBuffer(key);
	weight = (float*)mBuffer.GetCurrentWeightBuffer();
	grad = (float*)mBuffer.GetCurrentGradMergeBuffer();
	opt = make_shared<NAGTTOptimizer>(numAggregated, key, sz, 0x240, socketId);
}
OperationStatus PHubOptimizerOp::Run()
{
	CHECK(weight != NULL);
	CHECK(grad != NULL);
	CHECK(len != 0);
	opt->Update(targetKey, weight, grad, len);
	//reset. flip merge buffer.
	var mBuffer = pHub->RetrieveMergeBuffer(key);
	//Read and Write buffer have been collapsed into one buffer.
	//no need to flip.
	mBuffer.FlipBuffer();
	//if i am the optimizer, all i wanted is to send out the optimized value,
	//now weight has the current iteration of weight buffer, but it is pointed to by gradmergebuffer, which is used
	//to communicate.
	return OperationStatus::Finished;
}
