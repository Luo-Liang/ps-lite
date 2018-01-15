#pragma once
#include "phub.h"
#include <vector>

class OperatorContext
{
public:
	enum OperatorContextTypeCode
	{
		General,
		LocallyAvailable
	};
	OperatorContext(vector<BufferHandle> in,
		vector<BufferHandle> out) : Initialized(false)
	{
		inputs = in;
		outputs = out;
	}
	bool Initialized;
	vector<BufferHandle> inputs;
	vector<BufferHandle> outputs;
	shared_ptr<void*> additionalContext = NULL;
	OperatorContextTypeCode typeCode = OperatorContextTypeCode::General;
};

template <class T>
class LocallyAvailableOperatorContext : public OperatorContext
{
public:
	LocallyAvailableOperatorContext(vector<BufferHandle> in,
		vector<BufferHandle> out) 
		: OperatorContext(in, out)
	{
		typeCode = OperatorContext::OperatorContextTypeCode::LocallyAvailable;
	}
	vector<T*> inputAddrs;
	size_t lens;
	vector<T*> outputAddrs;
};
