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
	OperatorContext(vector<PLinkKey> in,
		vector<PLinkKey> out) : Initialized(false)
	{
		inputs = in;
		outputs = out;
	}
	bool Initialized;
	vector<PLinkKey> inputs;
	vector<PLinkKey> outputs;
	shared_ptr<void> additionalContext = NULL;
	OperatorContextTypeCode typeCode = OperatorContextTypeCode::General;
};

//template <class T>
//class LocallyAvailableOperatorContext : public OperatorContext
//{
//public:
//	LocallyAvailableOperatorContext(vector<PLinkKey> in,
//		vector<PLinkKey> out)
//		: OperatorContext(in, out)
//	{
//		typeCode = OperatorContext::OperatorContextTypeCode::LocallyAvailable;
//	}
//};

class PHubOperatorContext : public OperatorContext
{
public:
	NodeId From;
	NodeId To;
	PHubOperatorContext(vector<PLinkKey>& in, vector<PLinkKey>& out, NodeId from, NodeId to) :
		OperatorContext(in, out)
	{
		From = from;
		To = to;
	}
};

class GlooContext : public OperatorContext
{
public:
	GlooContext(vector<PLinkKey>& in, vector<PLinkKey>& out, int rank, int size) :
		OperatorContext(in, out)
	{
		Rank = rank;
		Size = size;
	}
	int Rank;
	int Size;
};
