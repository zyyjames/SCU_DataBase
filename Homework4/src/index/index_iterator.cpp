/**
 * Indexiterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"

namespace scudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>* InPage,int InIndex, BufferPoolManager* InManager) :
        Page(InPage), Index(InIndex), Manager(InManager) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() 
{
	Manager->UnpinPage(Page->GetPageId(),true);
}
INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() 
{
    return (!Page || (Index == Page->GetSize() && Page->GetNextPageId() == INVALID_PAGE_ID));
}

INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*() 
{
    return Page->GetItem(Index);
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE& INDEXITERATOR_TYPE::operator++() 
{
    Index++;
    if (Index == Page->GetSize() && Page->GetNextPageId() != INVALID_PAGE_ID) 
    {
        Index = 0;
        Manager->UnpinPage(Page->GetPageId(),true);
        Page = reinterpret_cast<BPlusTreeLeafPage<KeyType, ValueType,KeyComparator>*>(Manager->FetchPage(Page->GetNextPageId())->GetData());
    }
    return *this;
}
template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
