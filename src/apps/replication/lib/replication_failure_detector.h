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
# include <dsn/dist/failure_detector.h>

namespace dsn { namespace replication {

class replica_stub;
class replication_failure_detector  : public dsn::fd::failure_detector
{
public:
    replication_failure_detector(replica_stub* stub, std::vector<dsn_address_t>& meta_servers);
    ~replication_failure_detector(void);

    virtual void end_ping(::dsn::error_code err, const fd::beacon_ack& ack, void* context);

     // client side
    virtual void on_master_disconnected( const std::vector<dsn_address_t>& nodes );
    virtual void on_master_connected( const dsn_address_t& node);

    // server side
    virtual void on_worker_disconnected( const std::vector<dsn_address_t>& nodes ) { dassert (false, ""); }
    virtual void on_worker_connected( const dsn_address_t& node )  { dassert (false, ""); }

    dsn_address_t current_server_contact() const { zauto_lock l(_meta_lock); return _current_meta_server; }
    std::vector<dsn_address_t> get_servers() const  { zauto_lock l(_meta_lock); return _meta_servers; }

private:
    dsn_address_t find_next_meta_server(dsn_address_t current);

private:
    typedef std::set<dsn_address_t> end_points;

    mutable zlock           _meta_lock;
    dsn_address_t               _current_meta_server;

    std::vector<dsn_address_t>  _meta_servers;
    replica_stub            *_stub;
};

}} // end namespace

