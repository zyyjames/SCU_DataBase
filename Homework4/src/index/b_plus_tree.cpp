/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace scudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const 
{ 
    return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) 
{
    auto* LeafPage = FindLeafPage(key, false);// , Operation::READONLY, transaction);
    if (LeafPage) 
    {
        ValueType Value;
        if (LeafPage->Lookup(key, Value, comparator_)) 
        {
            result.push_back(Value);
            buffer_pool_manager_->UnpinPage(LeafPage->GetPageId(), false);
            return true;
        }
        buffer_pool_manager_->UnpinPage(LeafPage->GetPageId(), false);
    }
    return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) 
{
    if (IsEmpty()) 
    {
        StartNewTree(key, value);
        return true;
    }
    return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) 
{
    auto Root = reinterpret_cast<LEAFPAGE_TYPE*>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
    UpdateRootPageId(true);
    Root->Init(root_page_id_, INVALID_PAGE_ID);
    Root->Insert(key, value, comparator_);
    buffer_pool_manager_->UnpinPage(Root->GetPageId(), true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) 
{
    auto* Leaf = FindLeafPage(key, false);
    if (!Leaf) return false;
    ValueType v;
    if (Leaf->Lookup(key, v, comparator_)) 
    {
		buffer_pool_manager_->UnpinPage(Leaf->GetPageId(),false);
		return false;
	}
    if (Leaf->GetSize() < Leaf->GetMaxSize()) Leaf->Insert(key, value, comparator_);
    else 
    {
        auto* Leaf2 = Split<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>(Leaf);
        if (comparator_(key, Leaf2->KeyAt(0)) < 0) Leaf->Insert(key, value, comparator_);
        else Leaf2->Insert(key, value, comparator_);
        if (comparator_(Leaf->KeyAt(0), Leaf2->KeyAt(0)) < 0) {

            Leaf2->SetNextPageId(Leaf->GetNextPageId());
            Leaf->SetNextPageId(Leaf2->GetPageId());
        }
        else Leaf2->SetNextPageId(Leaf->GetPageId());
        InsertIntoParent(Leaf, Leaf2->KeyAt(0), Leaf2, transaction);
        buffer_pool_manager_->UnpinPage(Leaf2->GetPageId(),true);
    }
    buffer_pool_manager_->UnpinPage(Leaf->GetPageId(),true);
    return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) 
{ 
    page_id_t PageId;
    auto NewNode = reinterpret_cast<N*>(buffer_pool_manager_->NewPage(PageId)->GetData());
    NewNode->Init(PageId);
    node->MoveHalfTo(NewNode, buffer_pool_manager_);
    buffer_pool_manager_->UnpinPage(PageId,true);
    return NewNode;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) 
{
    if (old_node->IsRootPage()) 
    {
        auto Root = reinterpret_cast<INTERNALPAGE_TYPE*>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
        Root->Init(root_page_id_);
        Root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
        old_node->SetParentPageId(root_page_id_);
        new_node->SetParentPageId(root_page_id_);
        UpdateRootPageId(false);
        buffer_pool_manager_->UnpinPage(Root->GetPageId(), true);
    }
    else 
    {
        auto Internal = reinterpret_cast<INTERNALPAGE_TYPE*>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId())->GetData());
        if (Internal->GetSize() < Internal->GetMaxSize()) 
        {
            Internal->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
            new_node->SetParentPageId(Internal->GetPageId());
        }
        else 
        {
            page_id_t PageId;
            auto* Copy = reinterpret_cast<INTERNALPAGE_TYPE*>(buffer_pool_manager_->NewPage(PageId)->GetData());
            Copy->Init(PageId);
            Copy->SetSize(Internal->GetSize());
            for (int i = 1, j = 0; i <= Internal->GetSize(); ++i, ++j) 
            {
                if (Internal->ValueAt(i - 1) == old_node->GetPageId()) 
                {
                    Copy->SetKeyAt(j, key);
                    Copy->SetValueAt(j, new_node->GetPageId());
                    ++j;
                }
                if (i < Internal->GetSize()) 
                {
                    Copy->SetKeyAt(j, Internal->KeyAt(i));
                    Copy->SetValueAt(j, Internal->ValueAt(i));
                }
            }
            auto Internal2 = Split<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>(Copy);
            Internal->SetSize(Copy->GetSize() + 1);
            for (int i = 0; i < Copy->GetSize(); ++i) 
            {
                Internal->SetKeyAt(i + 1, Copy->KeyAt(i));
                Internal->SetValueAt(i + 1, Copy->ValueAt(i));
            }

            if (comparator_(key, Internal2->KeyAt(0)) < 0) new_node->SetParentPageId(Internal->GetPageId());
            else if (comparator_(key, Internal2->KeyAt(0)) == 0)  new_node->SetParentPageId(Internal2->GetPageId());
            else 
            {
                new_node->SetParentPageId(Internal2->GetPageId());
                old_node->SetParentPageId(Internal2->GetPageId());
            }
            buffer_pool_manager_->UnpinPage(Copy->GetPageId(), false);
            buffer_pool_manager_->DeletePage(Copy->GetPageId());
            InsertIntoParent(Internal, Internal2->KeyAt(0), Internal2);
        }
        buffer_pool_manager_->UnpinPage(Internal->GetPageId(), true);
    }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) 
{
    if (IsEmpty()) return;
    auto* Leaf = FindLeafPage(key, false);
    if (Leaf) 
    {
        int size_before_deletion = Leaf->GetSize();
        if (Leaf->RemoveAndDeleteRecord(key, comparator_) != size_before_deletion)
            CoalesceOrRedistribute(Leaf, transaction);
        buffer_pool_manager_->UnpinPage(Leaf->GetPageId(), true);
    }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) 
{
    if (node->IsRootPage()) return AdjustRoot(node);
    if (node->IsLeafPage()&& node->GetSize() >= node->GetMinSize()) return false;
    if (node->GetSize() > node->GetMinSize())  return false;
    auto Parent = reinterpret_cast<INTERNALPAGE_TYPE*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
    int ValueIndex = Parent->ValueIndex(node->GetPageId());
    int SiblingId = ValueIndex ? Parent->ValueAt(ValueIndex - 1) : Parent->ValueAt(ValueIndex + 1);
    auto* Page = buffer_pool_manager_->FetchPage(SiblingId);
    auto Sibling = reinterpret_cast<N*>(Page->GetData());
    if (Sibling->GetSize() + node->GetSize() > node->GetMaxSize()) 
    {
        buffer_pool_manager_->UnpinPage(Parent->GetPageId(), true);
        if (ValueIndex == 0) Redistribute<N>(Sibling, node, 1);
                buffer_pool_manager_->UnpinPage(Sibling->GetPageId(), true);
        return false;
    }
    if (ValueIndex == 0) 
    {
        Coalesce<N>(node, Sibling, Parent, 1, transaction);
        buffer_pool_manager_->UnpinPage(Parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(Sibling->GetPageId(), true);
        return false;
    }
    else 
    {
        Coalesce<N>(Sibling, node, Parent, ValueIndex, transaction);
        buffer_pool_manager_->UnpinPage(Parent->GetPageId(), true);
        buffer_pool_manager_->UnpinPage(Sibling->GetPageId(), true);
        return true;
    }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) 
{
    node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
    parent->Remove(index);
    return CoalesceOrRedistribute(parent, transaction);
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) 
{
    if (!index) neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
    else 
    {
        auto Parent =reinterpret_cast<INTERNALPAGE_TYPE*>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
        buffer_pool_manager_->UnpinPage(Parent->GetPageId(), false);
        neighbor_node->MoveLastToFrontOf(node, Parent->ValueIndex(node->GetPageId()), buffer_pool_manager_);
        buffer_pool_manager_->UnpinPage(Parent->GetPageId(), true);
    }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) 
{
    if (old_root_node->IsLeafPage()) 
    {
        if (!old_root_node->GetSize())
        {
            root_page_id_ = INVALID_PAGE_ID;
            UpdateRootPageId(false);
            return true;
        }
        return false;
    }
    if (old_root_node->GetSize() == 1)
    {
        root_page_id_ = reinterpret_cast<INTERNALPAGE_TYPE*>(old_root_node)->ValueAt(0);
        UpdateRootPageId(false);
        auto NewRoot =reinterpret_cast<INTERNALPAGE_TYPE*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
        NewRoot->SetParentPageId(INVALID_PAGE_ID);
        buffer_pool_manager_->UnpinPage(root_page_id_, true);
        return true;
    }
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() 
{ 
    KeyType key;
    return IndexIterator<KeyType, ValueType, KeyComparator>(FindLeafPage(key, true), 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) 
{
    auto* Leaf = FindLeafPage(key, false);
    int index = Leaf? Leaf->KeyIndex(key, comparator_):0;
    return IndexIterator<KeyType, ValueType, KeyComparator>(Leaf, index, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,bool leftMost) 
{
    if (IsEmpty()) return nullptr;
    auto* Node = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    //buffer_pool_manager_->UnpinPage(root_page_id_,false);
    while (!Node->IsLeafPage()) 
    {
        auto Internal = reinterpret_cast<INTERNALPAGE_TYPE*>(Node);
        buffer_pool_manager_->UnpinPage(Internal->GetPageId(),false);
        page_id_t ChildId =leftMost?Internal->ValueAt(0): Internal->Lookup(key, comparator_);       
        Node = reinterpret_cast<BPlusTreePage*>(buffer_pool_manager_->FetchPage(ChildId)->GetData());
    }
    return reinterpret_cast<LEAFPAGE_TYPE*>(Node);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace scudb
