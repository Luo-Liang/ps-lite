#pragma once
#define var auto
#define OUT 
#include <assert.h>
typedef size_t Cntr;
//returns 2 complement of the index at which the element should be placed on failure.
//returns the index at which the element present on success.
template<class Iter, class T>
int CxxxxBinarySearch(Iter begin, Iter end, T val)
{
	// Finds the lower bound in at most log(last - first) + 1 comparisons
	Iter i = std::lower_bound(begin, end, val);

	if (i != end && !(val < *i))
		return i - begin; // found
	else
		return ~(end - begin); // not found
}

std::vector<std::string> CxxxxStringSplit(const std::string& s, char delimiter)
{
	std::vector<std::string> tokens;
	std::string token;
	std::istringstream tokenStream(s);
	while (std::getline(tokenStream, token, delimiter))
	{
		tokens.push_back(token);
	}
	return tokens;
}

std::vector<int> approximateSetPartition(std::vector<float>& sizes, int setCount, vector<float>* setSizes = NULL)
{
	if (setCount == 0) return std::vector<int>();
	std::vector<std::pair<int, float>> sizePaired(sizes.size());
	for (size_t i = 0; i < sizes.size(); i++)
	{
		sizePaired[i].first = i;
		sizePaired[i].second = sizes[i];
	}
	std::sort(sizePaired.begin(), sizePaired.end(), [](const std::pair<int, int> &left, const std::pair<int, int> &right)
	{
		return left.second > right.second;
	});
	std::vector<float> accumulator(setCount, 0.0f);
	std::vector<int> result(sizes.size());
	//printf("sizes count = %d, set = %d\n", sizes.size(),  setCount);
	for (size_t i = 0; i < sizes.size(); i++)
	{
		/*auto it = std::min_element(accumulator.begin(), accumulator.end());
		auto idx = it - accumulator.begin();
		result.at(sizePaired[i].first) = idx;
		accumulator.at(idx) += sizes[i];*/
		//find the minimum in accumulator.
		int minIdx = 0;
		for (size_t acc = 0; acc < accumulator.size(); acc++)
		{
			if (accumulator.at(acc) < accumulator.at(minIdx))
			{
				minIdx = acc;
			}
		}
		result.at(sizePaired[i].first) = minIdx;
		accumulator.at(minIdx) += sizes[i];
		//printf("assigned %d = %f to set %d, set is currently %f\n", sizePaired[i].first, sizePaired[i].second, minIdx, accumulator.at(minIdx));
		//assign this to the minimum buckets.
	}
	if (setSizes != NULL)
	{
		*setSizes = accumulator;
	}
	return result;
}

template <class T, class P, class Selector>
std::vector<T> CxxxxSelect(std::vector<P>& in, Selector op)
{
	vector<T> result;
	result.resize(in.size());
	std::transform(in.begin(), in.end(), std::back_inserter(result), op);
	return result;
}


template <class T>
bool AllEqual(vector<T> elements)
{
	if (std::adjacent_find(elements.begin(), elements.end(), std::not_equal_to<T>()) == elements.end())
	{
		return true;
	}
	else
	{
		return false;
	}
}



template< typename... Args >
std::string CxxxxStringFormat(const char* format, Args... args) {
	int length = std::snprintf(nullptr, 0, format, args...);
	assert(length >= 0);

	char* buf = new char[length + 1];
	std::snprintf(buf, length + 1, format, args...);

	std::string str(buf);
	delete[] buf;
	return std::move(str);
}

void ParseHostPort(std::string combo, OUT std::string& host, OUT uint& port)
{
	auto redisVec = CxxxxStringSplit(host, ':');
	assert(redisVec.size() == 2);
	host = redisVec[0];
	port = atoi(redisVec[1].c_str());
	assert(port != -1);
}