/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
namespace scudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) 
{
    page_type_=IndexPageType::LEAF_PAGE;
    size_=0;
    page_id_=page_id;
    parent_page_id_=parent_id;
    next_page_id_=INVALID_PAGE_ID;
    max_size_ = (PAGE_SIZE - sizeof(BPlusTreeLeafPage)) /(sizeof(KeyType) + sizeof(ValueType));
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const 
{
    return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) 
{
    next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const
{
	if(!size_||comparator(key,array[size_-1].first)>0)
	{
		return size_;
	}
    int Low = 0;
    int High = size_;
    int Mid=(High + Low) / 2;
    while (Low < High-1) 
    {
        if (comparator(array[Mid].first,key)>0) High = Mid;
        else if (comparator(array[Mid].first,key)<0) Low = Mid;
        else return Mid;
        Mid = (High + Low) / 2;
    }
    return comparator(array[Mid].first,key)>=0?Mid:High;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const 
{
    return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) 
{
    return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) 
{
    int SearchKeyIndex = KeyIndex(key, comparator);
    if (comparator(key,KeyAt(SearchKeyIndex)))
    {
        for (int i = size_; i > SearchKeyIndex; i--)
        {
            array[i] = array[i-1];
        }
        size_++;
        array[SearchKeyIndex].first = key;
        array[SearchKeyIndex].second = value;
    }
    return size_;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) 
{
    int HalfSize = size_ / 2;
    MappingType* src = array + size_ - HalfSize;
    recipient->CopyHalfFrom(src, HalfSize);
    size_ -= HalfSize;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) 
{
    for (int i = 0; i < size; i++) 
    {
        array[i] = *(items++);
    }
    size_+=size;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const 
{
    if(!size_)return false;
    int SearchKeyIndex = KeyIndex(key, comparator);
    if(SearchKeyIndex==size_)return false;
    if (SearchKeyIndex<size_&&!comparator(key,KeyAt(SearchKeyIndex)))
    {
        value = array[SearchKeyIndex].second;
        return true;
    }
    return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) 
{
    int SearchKeyIndex = KeyIndex(key, comparator);
    if (!comparator(key,KeyAt(SearchKeyIndex)))
    {
        for (int i = SearchKeyIndex; i < size_-1; i++)
        {
            array[i] = array[i + 1];
        }
        size_--;
    }
    return size_;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) 
{
    recipient->CopyAllFrom(array, size_);
    recipient->SetNextPageId(next_page_id_);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) 
{
    for (int i = 0; i < size; i++) {
        array[size_ + i] = *(items++);
    }
    size_ += size;
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
    MappingType& First = array[0];
    size_--;
    memmove(array, array + 1, size_ * sizeof(MappingType));
    recipient->CopyLastFrom(First);
    auto Parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(buffer_pool_manager->FetchPage(parent_page_id_)->GetData());
    Parent->SetKeyAt(Parent->ValueIndex(page_id_), First.first);
    buffer_pool_manager->UnpinPage(parent_page_id_, true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) 
{
    array[size_] = item;
    size_++;
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
    size_--;
    recipient->CopyFirstFrom(array[size_], parentIndex, buffer_pool_manager);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) 
{
    memmove(array + 1, array, size_ * sizeof(MappingType));
    size_++;
    array[0] = item;
    auto Parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(buffer_pool_manager->FetchPage(parent_page_id_)->GetData());
    Parent->SetKeyAt(parentIndex, item.first);
    buffer_pool_manager->UnpinPage(parent_page_id_, true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (size_ == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << page_id_ << " parentId: " << parent_page_id_
           << "]<" << size_ << "> ";
  }
  int entry = 0;
  int end = size_;
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace scudb
