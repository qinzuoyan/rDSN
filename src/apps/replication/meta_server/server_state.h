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
#include <set>

using namespace dsn;
using namespace dsn::service;
using namespace dsn::replication;

namespace dsn {
    namespace replication{
        class replication_checker;
    }
}

typedef std::list<std::pair<dsn_address_t, bool>> node_states;

struct app_state
{
    std::string                          app_type;
    std::string                          app_name;
    int32_t                              app_id;
    int32_t                              partition_count;
    std::vector<partition_configuration> partitions;
};

typedef std::unordered_map<global_partition_id, std::shared_ptr<configuration_update_request> > machine_fail_updates;

class server_state 
{
public:
    server_state(void);
    ~server_state(void);

    void init_app();

    void get_node_state(__out_param node_states& nodes);
    void set_node_state(const node_states& nodes, __out_param machine_fail_updates* pris);
    bool get_meta_server_primary(__out_param dsn_address_t& node);

    void add_meta_node(const dsn_address_t& node);
    void remove_meta_node(const dsn_address_t& node);
    void switch_meta_primary();

    void load(const char* chk_point);
    void save(const char* chk_point);

    // partition server & client => meta server
    void query_configuration_by_node(configuration_query_by_node_request& request, __out_param configuration_query_by_node_response& response);
    void query_configuration_by_index(configuration_query_by_index_request& request, __out_param configuration_query_by_index_response& response);
    void query_configuration_by_gpid(global_partition_id id, __out_param partition_configuration& config);
    void update_configuration(configuration_update_request& request, __out_param configuration_update_response& response);
    void unfree_if_possible_on_start();
    bool freezed() const { return _freeze.load(); }
    
private:
    void check_consistency(global_partition_id gpid);
    void update_configuration_internal(configuration_update_request& request, __out_param configuration_update_response& response);

private:
    friend class ::dsn::replication::replication_checker;

    struct node_state
    {
        bool                          is_alive;
        dsn_address_t                address;
        std::set<global_partition_id> primaries;
        std::set<global_partition_id> partitions;
    };

    mutable zrwlock_nr                   _lock;
    std::unordered_map<dsn_address_t, node_state>   _nodes;
    std::vector<app_state>            _apps;

    int                               _node_live_count;
    int                               _node_live_percentage_threshold_for_update;
    std::atomic<bool>                 _freeze;

    mutable zrwlock_nr                _meta_lock;
    std::vector<dsn_address_t>       _meta_servers;
    int                               _leader_index;

    friend class load_balancer;
};

