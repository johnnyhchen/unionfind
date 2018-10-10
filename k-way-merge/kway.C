#include "kway.decl.h"
#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include <vector>

#define NUM_GENERATORS		10

CkReduction::reducerType mergeCountMapsReductionType;

struct entry_ptr_cmp {
	bool operator()(entry*& a, entry*& b) {
		// Heaps place the largest element on top, so sort descendingly to get smallest id on top
		return a->id > b->id;
	}
};

CkReductionMsg *merge_count_maps(int nMsgs, CkReductionMsg **msgs) {
	// Heap structure based on entry ID's
	std::vector<entry *> heap;
	// Pre-reserve the amount of space we need for the structure
	heap.reserve(nMsgs);

	// Comparison function used between members of the heap
	auto comparator = entry_ptr_cmp();
	// Map of entry pointer to the array it originated from (from the CkReductionMsg** param)
	std::unordered_map<entry *, int> entryToArrayIdx;
	entryToArrayIdx.reserve(nMsgs);
	// Index to pull the next element from in each array
	int arrayPosition[nMsgs];
	// The result vector of entries
	std::vector<entry> result;

	int totalNumElements = 0;
	for (int i = 0; i < nMsgs; i++) {
		heap.push_back((entry *) (msgs[i]->getData()));
		entryToArrayIdx[(entry *)msgs[i]->getData()] = i;
		arrayPosition[i] = 1;
		int dataSize = msgs[i]->getSize();
		int numElems = dataSize / sizeof(entry);
		totalNumElements += numElems;
	}

	// Convert the heap vector into an actual heap
	std::make_heap(heap.begin(), heap.end(), comparator);

	while (heap.size() > 0) {
		uint32_t id = heap[0]->id;
		uint32_t id_count = 0;
		while (id == heap[0]->id && totalNumElements > 0) {
			auto tmp = heap[0];
			id_count += tmp->count;
			// pop_heap moves the frontmost element to the end and reorders the rest of the heap
			std::pop_heap(heap.begin(), heap.end(), comparator);
			heap.pop_back();
			totalNumElements--;
			int arrayPullId = entryToArrayIdx[tmp];
			if (arrayPosition[arrayPullId] * sizeof(entry) != msgs[arrayPullId]->getSize()) {
				// After we add an element from array i, we should push the next element from array i onto the heap
				heap.push_back(&(((entry *)(msgs[arrayPullId]->getData()))[arrayPosition[arrayPullId]]));
				arrayPosition[arrayPullId]++;
				// erasings and additions to a large enough map operate in constant time
				entryToArrayIdx.erase(tmp);
				entryToArrayIdx[heap.back()] = arrayPullId;
				push_heap(heap.begin(), heap.end(), comparator);
			}
		}
		entry next_entry = {id, id_count};
		result.push_back(next_entry);
	}
	entry *res_array = new entry[result.size()];
	std::copy(result.begin(), result.end(), res_array);
	auto res = CkReductionMsg::buildNew(result.size() * sizeof(entry), res_array);
	return res;
}

static void register_merge_counts_reduction() {
	mergeCountMapsReductionType = CkReduction::addReducer(merge_count_maps);
}

class Main : public CBase_Main {
private:
	CProxy_Generator elems;
public:
	Main(CkArgMsg *m) {
		elems = CProxy_Generator::ckNew(thisProxy, NUM_GENERATORS);
		elems.start();
	}

	void merge_count_results(CkReductionMsg *msg) {
		entry *entries = (entry *) (msg->getData());
		int size = msg->getSize();
		for (int i = 0; i < size / sizeof(entry); i++) {
			CkPrintf("ID=%d, count=%d\n", entries[i].id, entries[i].count);
		}
		CkExit();
	}
};

class Generator : public CBase_Generator {
private:
	CProxy_Main mainProxy;
public:
	Generator(CkMigrateMessage *m) {}
	Generator(CProxy_Main mproxy) {
		mainProxy = mproxy;
	}

	void start() {
		srand(time(NULL) + thisIndex);
		int numElems = (unsigned int)rand() % 13u + 1u;
		entry elems[numElems];
		uint32_t cur_id = 1;
		for (int i = 0; i < numElems; i++) {
			elems[i] = {cur_id, 1 + (unsigned int)rand() % 10u};
			cur_id += rand() % 5 + 1;
		}
		CkCallback cb(CkIndex_Main::merge_count_results(NULL), this->thisProxy);
		int size = numElems * sizeof(entry);
		contribute(size, elems, mergeCountMapsReductionType, cb);
	}
};

#include "kway.def.h"