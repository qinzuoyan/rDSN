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

/*
 * Description:
 *     monitor client
 *
 * Revision history:
 *     2015-01-21, qinzuoyan, first version
 */

#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication_other_types.h>
#include "monitor_client.h"
#include <iostream>
#include <fstream>
#include <iomanip>

namespace dsn{ namespace replication{

monitor_client::monitor_client(const std::vector<dsn::rpc_address>& meta_servers)
{
    _meta_servers.assign_group(dsn_group_build("meta.servers"));
    for (auto& m : meta_servers)
        dsn_group_add(_meta_servers.group_handle(), m.c_addr());
}

dsn::error_code monitor_client::list_apps(std::vector<app_info>& apps)
{
    std::shared_ptr<configuration_list_apps_request> req(new configuration_list_apps_request());
    req->status = AS_ALL;

    auto resp_task = request_meta<configuration_list_apps_request>(
            RPC_CM_LIST_APPS,
            req
    );

    // TODO(qinzuoyan):  resp_task->wait() without timeout will hang
    bool wait_ret = resp_task->wait(3000);
    if (!wait_ret)
    {
        resp_task->cancel(false);
        return ERR_TIMEOUT;
    }

    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_list_apps_response resp;
    ::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    apps.swap(resp.infos);

    return dsn::ERR_OK;
}

dsn::error_code monitor_client::list_app(const std::string& app_name,
                                     int32_t& app_id,
                                     std::vector< partition_configuration>& partitions)
{
    std::shared_ptr<configuration_query_by_index_request> req(new configuration_query_by_index_request());
    req->app_name = app_name;

    auto resp_task = request_meta<configuration_query_by_index_request>(
            RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
            req
    );

    bool wait_ret = resp_task->wait(3000);
    if (!wait_ret)
    {
        resp_task->cancel(false);
        return ERR_TIMEOUT;
    }

    if (resp_task->error() != dsn::ERR_OK)
    {
        return resp_task->error();
    }

    dsn::replication::configuration_query_by_index_response resp;
    ::unmarshall(resp_task->response(), resp);
    if(resp.err != dsn::ERR_OK)
    {
        return resp.err;
    }

    app_id = resp.app_id;
    partitions.swap(resp.partitions);

    return dsn::ERR_OK;
}

void monitor_client::end_meta_request(task_ptr callback, int retry_times, error_code err, dsn_message_t request, dsn_message_t resp)
{
    ddebug("end_meta_request(): err=%s, retry_times=%d", err.to_string(), retry_times);
    if(err == dsn::ERR_TIMEOUT && retry_times < 2)
    {
        rpc_address leader = dsn_group_get_leader(_meta_servers.group_handle());
        rpc_address next = dsn_group_next(_meta_servers.group_handle(), leader.c_addr());
        dsn_group_set_leader(_meta_servers.group_handle(), next.c_addr());

        rpc::call(
            _meta_servers,
            request,
            this,
            [=, callback_capture = std::move(callback)](error_code err, dsn_message_t request, dsn_message_t response)
            {
                end_meta_request(std::move(callback_capture), retry_times + 1, err, request, response);
            },
            0
         );
    }
    else
        callback->enqueue_rpc_response(err, resp);
}

}} // namespace
