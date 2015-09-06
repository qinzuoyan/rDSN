/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#pragma once

#include "replication_common.h"
#include "mutation.h"

namespace dsn { namespace replication {

#define INVALID_FILENUMBER (0)
#define MAX_LOG_FILESIZE (32)

class log_file;
typedef dsn::ref_ptr<log_file> log_file_ptr;

typedef std::unordered_map<global_partition_id, decree> multi_partition_decrees;

struct log_block_header
{
    int32_t magic;
    int32_t length;
    int32_t body_crc;
    int32_t padding;
};

struct log_file_header
{
    int32_t  magic;
    int32_t  version;
    int32_t  header_size;
    int32_t  max_staleness_for_commit;
    int32_t  log_buffer_size_bytes;
    int64_t  start_global_offset;
};

class mutation_log : public virtual servicelet
{
public:
    // mutationPtr
    typedef std::function<void (mutation_ptr&)> replay_callback;

public:
    //
    // ctors 
    //
    mutation_log(
        uint32_t log_buffer_size_mb, 
        uint32_t log_pending_max_ms, 
        uint32_t max_log_file_mb = (uint64_t) MAX_LOG_FILESIZE, 
        bool batch_write = true, 
        int write_task_max_count = 2
        );
    virtual ~mutation_log();
    
    //
    // initialization
    //
    error_code initialize(const char* dir);
    error_code replay(replay_callback callback);
    void reset();
    error_code start_write_service(multi_partition_decrees& initMaxDecrees, int max_staleness_for_commit);
    void close();
       
    //
    // log mutation
    //
    // return value: nullptr for error
    ::dsn::task_ptr append(mutation_ptr& mu,
            dsn_task_code_t callback_code,
            servicelet* callback_host,
            aio_handler callback,
            int hash = 0);

    // Remove entry <gpid, decree> from m_initPreparedDecrees when a partition is removed. 
    void on_partition_removed(global_partition_id gpid);

    //
    //  garbage collection logs that are already covered by durable state on disk, return deleted log segment count
    //
    int garbage_collection(multi_partition_decrees& durable_decrees, multi_partition_decrees& max_seen_decrees);

    //
    //    other inquiry routines
    const std::string& dir() const {return _dir;}
    int64_t            end_offset() const { return _global_end_offset; }
    int64_t            start_offset() const { return _global_start_offset; }

    std::map<int, log_file_ptr>& get_logfiles_for_test();

private:
    //
    //  internal helpers
    //
    typedef std::shared_ptr<std::list<::dsn::task_ptr>> pending_callbacks_ptr;

    error_code create_new_log_file();
    void create_new_pending_buffer();    
    void internal_pending_write_timer(binary_writer* w_ptr);
    static void internal_write_callback(error_code err, size_t size, pending_callbacks_ptr callbacks, blob data);
    error_code write_pending_mutations(bool create_new_log_when_necessary = true);

private:    
    
    zlock                     _lock;
    int64_t                   _max_log_file_size_in_bytes;            
    std::string               _dir;    
    bool                      _batch_write;

    // write & read
    int                         _last_file_number;
    std::map<int, log_file_ptr> _log_files;
    log_file_ptr                _last_log_file;
    log_file_ptr                _current_log_file;
    int64_t                     _global_start_offset;
    int64_t                     _global_end_offset;
    
    // gc
    multi_partition_decrees     _init_prepared_decrees;
    int                         _max_staleness_for_commit;

    // bufferring
    uint32_t                    _log_buffer_size_bytes;
    uint32_t                    _log_pending_max_milliseconds;
    
    std::shared_ptr<binary_writer> _pending_write;
    pending_callbacks_ptr          _pending_write_callbacks;
    ::dsn::task_ptr   _pending_write_timer;
    
    int                         _write_task_number;
};

class log_file : public ref_counter
{
public:    
    ~log_file() { close(); }

    //
    // file operations
    //
    static log_file_ptr opend_read(const char* path);
    static log_file_ptr create_write(const char* dir, int index, int64_t startOffset, int max_staleness_for_commit, int write_task_max_count = 2);
    void close();

    //
    // read routines
    //
    error_code read_next_log_entry(__out_param::dsn::blob& bb);

    //
    // write routines
    //
    // return value: nullptr for error or immediate success (using ::GetLastError to get code), otherwise it is pending
    ::dsn::task_ptr write_log_entry(
                    blob& bb,
                    dsn_task_code_t evt,  // to indicate which thread pool to execute the callback
                    servicelet* callback_host,
                    aio_handler callback,
                    int64_t offset,
                    int hash
                    );
    
    // others
    int64_t end_offset() const { return _end_offset; }
    int64_t start_offset() const  { return _start_offset; }
    int   index() const { return _index; }
    const std::string& path() const { return _path; }
    const multi_partition_decrees& init_prepare_decrees() { return _init_prepared_decrees; }
    const log_file_header& header() const { return _header;}

    int read_header(binary_reader& reader);
    int write_header(binary_writer& writer, multi_partition_decrees& initMaxDecrees, int bufferSizeBytes);
    bool is_right_header() const;
    
private:
    log_file(const char* path, dsn_handle_t handle, int index, int64_t startOffset, int max_staleness_for_commit, bool isRead, int write_task_max_count = 2);

protected:        
    int64_t       _start_offset;
    int64_t       _end_offset;
    dsn_handle_t      _handle;
    bool          _is_read;
    std::string   _path;
    int           _index;
    std::vector<::dsn::task_ptr>  _write_tasks;
    int                        _write_task_itr;    

    // for gc
    multi_partition_decrees _init_prepared_decrees;    
    log_file_header         _header;
};

}} // namespace
