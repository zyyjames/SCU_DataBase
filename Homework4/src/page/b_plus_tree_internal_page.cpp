/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"

namespace scudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) 
{
    page_type_ = IndexPageType::INTERNAL_PAGE;
    size_=1;   
    page_id_ = page_id;
    parent_page_id_ = parent_id;
    max_size_ = (PAGE_SIZE - sizeof(BPlusTreeInternalPage)) / (sizeof(KeyType) + sizeof(ValueType));
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const 
{
  // replace with your own code
    return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) 
{
    array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const 
{
    for (int i = 0; i < size_; i++) 
    {
        if (array[i].second == value)
            return i;
    }
    return size_;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const 
{
    return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const 
{
	if(comparator(key,array[1].first)<0)return array[0].second;
	else if(comparator(key,array[size_-1].first)>=0)return array[size_-1].second;
    int Low = 1;
    int High = size_-1;
    int Mid;
    while (Low < High-1)
    {
        Mid = (High + Low) / 2;
        if (comparator(key, array[Mid].first) < 0) High = Mid;
        else if (comparator(key, array[Mid].first) > 0)Low = Mid;
        else return array[Mid].second;
    }
    return array[Low].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
    array[0].second = old_value;
    array[1] = { new_key, new_value };
    size_++;
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) 
{
    for (int i = size_; i > 0; i--) 
    {
        if (array[i - 1].second == old_value) 
        {
            array[i] = { new_key, new_value };
            size_++;
            break;
        }
        array[i] = array[i - 1];
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
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
    int HalfSize = (size_+1)/2;
    recipient->CopyHalfFrom(array + size_-HalfSize, HalfSize, buffer_pool_manager);

    for (int i = size_ - HalfSize; i < size_; i++) 
    {
        auto Child = FetchPage(buffer_pool_manager, ValueAt(i));
        Child->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(Child->GetPageId(), true);
    }
    size_-=HalfSize;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) 
{
    for (int i = 0; i < size; i++) 
    {
        array[i] = *(items++);
    }
    size_ += size-1;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) 
{
    for (int i = index; i < size_ - 1; i++) 
    {
        array[i] = array[i + 1];
    }
    size_--;
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() 
{
    size_--;
    return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) 
{
    auto* Parent = FetchInternalPage(buffer_pool_manager,parent_page_id_);
    SetKeyAt(0, Parent->KeyAt(index_in_parent));
    assert(Parent->ValueAt(index_in_parent) == GetPageId());
    buffer_pool_manager->UnpinPage(Parent->GetPageId(), true);
    recipient->CopyAllFrom(array, size_, buffer_pool_manager);
    for (int i = 0; i < size_; i++) 
    {
        auto Child = FetchPage(buffer_pool_manager,ValueAt(i));
        Child->SetParentPageId(recipient->GetPageId());
        buffer_pool_manager->UnpinPage(Child->GetPageId(), true);
    }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) 
{
    for (int i = 0; i < size; i++) {
        array[size_ + i] = *items++;
    }
    size_+=size;
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) 
{
    MappingType First{ KeyAt(1), ValueAt(0) };
    SetValueAt(0, ValueAt(1));
    Remove(1);
    recipient->CopyLastFrom(First, buffer_pool_manager);
    auto Child = FetchPage(buffer_pool_manager, ValueAt(0));
    Child->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(Child->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) 
{
    auto Parent = FetchInternalPage(buffer_pool_manager, parent_page_id_);
    auto Index = Parent->ValueIndex(page_id_);
    auto Key = Parent->KeyAt(Index + 1);
    array[size_] = { Key, pair.second };
    size_++;
    Parent->SetKeyAt(Index + 1, pair.first);
    buffer_pool_manager->UnpinPage(Parent->GetPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) 
{
    size_--;
    MappingType Pair = array[size_];
    recipient->CopyFirstFrom(Pair, parent_index, buffer_pool_manager);
    auto Child = FetchPage(buffer_pool_manager,Pair.second);
    Child->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(Child->GetPageId(), true);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) 
{
    auto Parent = FetchInternalPage(buffer_pool_manager,parent_page_id_);
    Parent->SetKeyAt(parent_index, pair.first);
    InsertNodeAfter(array[0].second, Parent->KeyAt(parent_index), array[0].second);
    array[0].second = pair.second;
    buffer_pool_manager->UnpinPage(Parent->GetPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < size_; i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (size_ == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << page_id_ << " parentId: " << GetParentPageId()
       << "]<" << size_ << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = size_;
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace scudb
