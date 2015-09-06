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
#include "load_balancer.h"
#include <algorithm>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "load.balancer"

load_balancer::load_balancer(server_state* state)
: _state(state), serverlet<load_balancer>("load_balancer")
{
}

load_balancer::~load_balancer()
{
}

void load_balancer::run()
{
    zauto_read_lock l(_state->_lock);

    for (size_t i = 0; i < _state->_apps.size(); i++)
    {
        app_state& app = _state->_apps[i];
        
        for (int j = 0; j < app.partition_count; j++)
        {
            partition_configuration& pc = app.partitions[j];
            run_lb(pc);
        }
    }
}

void load_balancer::run(global_partition_id gpid)
{
    zauto_read_lock l(_state->_lock);
    partition_configuration& pc = _state->_apps[gpid.app_id - 1].partitions[gpid.pidx];
    run_lb(pc);
}

dsn_address_t load_balancer::find_minimal_load_machine(bool primaryOnly)
{
    std::vector<std::pair<dsn_address_t, int>> stats;

    for (auto it = _state->_nodes.begin(); it != _state->_nodes.end(); it++)
    {
        if (it->second.is_alive)
        {
            stats.push_back(std::make_pair(it->first, static_cast<int>(primaryOnly ? it->second.primaries.size()
                : it->second.partitions.size())));
        }
    }

    
    std::sort(stats.begin(), stats.end(), [](const std::pair<dsn_address_t, int>& l, const std::pair<dsn_address_t, int>& r)
    {
        return l.second < r.second;
    });

    if (stats.empty())
    {
        return dsn_address_invalid;
    }

    int candidate_count = 1;
    int val = stats[0].second;

    for (size_t i = 1; i < stats.size(); i++)
    {
        if (stats[i].second > val)
            break;
        candidate_count++;
    }

    return stats[dsn_random32(0, candidate_count - 1)].first;
}

void load_balancer::run_lb(partition_configuration& pc)
{
    if (_state->freezed())
        return;

    configuration_update_request proposal;
    proposal.config = pc;

    if (pc.primary == dsn_address_invalid)
    {
        if (pc.secondaries.size() > 0)
        {
            proposal.node = pc.secondaries[dsn_random32(0, static_cast<int>(pc.secondaries.size()) - 1)];
            proposal.type = CT_UPGRADE_TO_PRIMARY;
        }
        else
        {
            proposal.node = find_minimal_load_machine(true);
            proposal.type = CT_ASSIGN_PRIMARY;
        }

        if (proposal.node != dsn_address_invalid)
        {
            send_proposal(proposal.node, proposal);
        }
    }

    else if (static_cast<int>(pc.secondaries.size()) + 1 < pc.max_replica_count)
    {
        proposal.type = CT_ADD_SECONDARY;
        proposal.node = find_minimal_load_machine(false);
        if (proposal.node != dsn_address_invalid && 
            proposal.node != pc.primary &&
            std::find(pc.secondaries.begin(), pc.secondaries.end(), proposal.node) == pc.secondaries.end())
        {
            send_proposal(pc.primary, proposal);
        }
    }
    else
    {
        // it is healthy, nothing to do
    }
}

// meta server => partition server
void load_balancer::send_proposal(const dsn_address_t& node, const configuration_update_request& proposal)
{
    dinfo("send proposal %s of %s:%hu, current ballot = %lld", 
        enum_to_string(proposal.type),
        proposal.node.name,
        proposal.node.port,
        proposal.config.ballot
        );

    rpc::call_one_way_typed(node, RPC_CONFIG_PROPOSAL, proposal, gpid_to_hash(proposal.config.gpid));
}

void load_balancer::query_decree(std::shared_ptr<query_replica_decree_request> query)
{
    rpc::call_typed(query->node, RPC_QUERY_PN_DECREE, query, this, &load_balancer::on_query_decree_ack, gpid_to_hash(query->gpid), 3000);
}

void load_balancer::on_query_decree_ack(error_code err, std::shared_ptr<query_replica_decree_request>& query, std::shared_ptr<query_replica_decree_response>& resp)
{
    if (err != ERR_OK)
    {
        tasking::enqueue(LPC_QUERY_PN_DECREE, this, std::bind(&load_balancer::query_decree, this, query), 0, 1000);
    }
    else
    {
        zauto_write_lock l(_state->_lock);
        app_state& app = _state->_apps[query->gpid.app_id - 1];
        partition_configuration& ps = app.partitions[query->gpid.pidx];
        if (resp->last_decree > ps.last_committed_decree)
        {
            ps.last_committed_decree = resp->last_decree;
        }   
    }
}
