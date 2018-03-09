#include <ps/PHubAggregator.h>
#include <ps/Operator.h>
#include <ps/OperatorContext.h>

//class PHubAggregator :IOperator
//{
//	shared_ptr<PHub> pPhub = NULL;
//	PLinkKey key;
//	size_t keySize;
//	shared_ptr<OperatorContext> opContext;
//public:
//	virtual void Initialize(shared_ptr<OperatorContext> context) override;
//	virtual OperationStatus Run() override;
//};


class Aggregator
{
public:
	//static Aggregator* Create(std::string name, const size_t prefetch_dist);

	//add vector 2 (src) to vector 1 (dst)
	virtual void VectorVectorAdd(float* dst, size_t len, float* src)
	{
		assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
		for (size_t m = 0; m < len; m += INSTRUCTION_VECTOR_SIZE) {
			_mm_store_ps(dst + m, _mm_add_ps(_mm_load_ps(dst + m), _mm_load_ps(src + m)));
		}
	}

protected:
	const size_t prefetch_distance;

	Aggregator(const size_t prefetch_distance = 0x240)
		: prefetch_distance(prefetch_distance)
	{ }

	virtual ~Aggregator() { }
};

class TTAggregator : public Aggregator
{
public:
	TTAggregator(const size_t prefetch_distance = 0x240)
		: Aggregator(prefetch_distance)
	{ }

	virtual void VectorVectorAdd(float* dst, size_t len, float* src)
	{
		assert((len & INSTRUCTION_VECTOR_SIZE_MASK) == 0);
		size_t offset = 0;
		size_t prefetch_offset = prefetch_distance;
		__asm__ __volatile__(".align 32 \n\t"
			"1: \n\t"

			"movaps 0x00(%[dst],%[offset],1),%%xmm0 \n\t"
			"movaps 0x10(%[dst],%[offset],1),%%xmm1 \n\t"
			"movaps 0x20(%[dst],%[offset],1),%%xmm2 \n\t"
			"movaps 0x30(%[dst],%[offset],1),%%xmm3 \n\t"

			"addps  0x00(%[src],%[offset],1),%%xmm0 \n\t"
			"addps  0x10(%[src],%[offset],1),%%xmm1 \n\t"
			"addps  0x20(%[src],%[offset],1),%%xmm2 \n\t"
			"addps  0x30(%[src],%[offset],1),%%xmm3 \n\t"

			"movaps  %%xmm0,0x00(%[dst],%[offset],1) \n\t"
			"movaps  %%xmm1,0x10(%[dst],%[offset],1) \n\t"
			"movaps  %%xmm2,0x20(%[dst],%[offset],1) \n\t"
			"movaps  %%xmm3,0x30(%[dst],%[offset],1) \n\t"

			"add %[inc],%[offset] \n\t"

			"cmp %[end], %[offset] \n\t"
			"jb 1b \n\t"

			"sfence \n\t"

			: [offset] "+mr" (offset),
			[prefetch_offset] "+mr" (prefetch_offset)
			: [dst] "c" (dst), // Use rcx to keep insns small
			[src] "D" (src), // Use rdi to keep insns small
			[inc] "i" (CACHELINE_SIZE_BYTES),
			[end] "mr" (len * sizeof(float))
			: "cc", "memory",
			"xmm0", "xmm1", "xmm2", "xmm3",
			"xmm4", "xmm5", "xmm6", "xmm7");
	}
};

void PHubAggregator::Initialize(shared_ptr<OperatorContext> context)
{
	pPhub = dynamic_pointer_cast<PHub>(context->additionalContext);
	key = context->inputs.at(0);
	keySize = pPhub->keySizes.at(key);
	opContext = context;
	aggregator = make_shared<TTAggregator>();
	//figure out where is the source and where is the destination.
	var mBuffer = pPhub->RetrieveMergeBuffer(key);
	dest = (float*)mBuffer.GetCurrentWriteBuffer();
	//consult allocator for source.
	var& allocator = pPhub->GetAllocator();
	source = 
}

OperationStatus PHubAggregator::Run()
{
	CHECK(aggregator != NULL);
	//dest, source, len
	//need to saveit in the merge buffer's write location.
	aggregator->VectorVectorAdd( ,keySize, );
	return OperationStatus::Finished;
}