#include "buffer/buffer_pool_manager.h"
#include <iostream>
namespace scudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) 
{ 
    Page* page=nullptr;
    page_table_->Find(page_id, page);
    if (!page)
    {
        if (free_list_->size())
        {
            page = free_list_->back();
            free_list_->pop_back();
        }
        else
        {
            replacer_->Victim(page);
            if (!page)return nullptr;
            else if (page->is_dirty_)
            {
                disk_manager_->WritePage(page->page_id_, page->GetData());
            }
            page_table_->Remove(page->page_id_);
        }
        page->page_id_ = page_id;
        page_table_->Insert(page_id, page);
    }
    //replacer_->Insert(page);
    disk_manager_->ReadPage(page->page_id_, page->GetData());
    page->pin_count_++;
    return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
    Page* Page = nullptr;
    page_table_->Find(page_id, Page);
    if (!Page || Page->pin_count_ <= 0)return false;
    Page->is_dirty_ = is_dirty;
    Page->pin_count_--;
    //cout<<Page->pin_count_<<endl;
    if (!Page->pin_count_)replacer_->Insert(Page);
    return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { return false; }

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { return false; }

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) 
{ 
    Page* page=nullptr;
    page_id = disk_manager_->AllocatePage();
    if (free_list_->size())
    {
        page = *free_list_->begin();
        free_list_->pop_front();
    }
    else
    {
        replacer_->Victim(page);
        //cout<<page->pin_count_<<endl;
        if (!page)return nullptr;
        else if (page->is_dirty_)
        {
            disk_manager_->WritePage(page->page_id_, page->GetData());
        }
        page_table_->Remove(page->page_id_);
    }   
    page->page_id_ = page_id;
    page->pin_count_++;
    page_table_->Insert(page_id, page);
    //replacer_->Insert(page);
    return page;
}
} // namespace scudb
