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

#pragma once

#include <cctype>
#include <dsn/dist/replication.h>
#include "../../../core/core/group_address.h"

namespace dsn{ namespace replication{

class monitor_client : public clientlet
{
public:
    monitor_client(const std::vector<dsn::rpc_address>& meta_servers);

    dsn::rpc_address primary_meta_server() { return _meta_servers.group_address()->leader(); }

    dsn::error_code list_apps(std::vector<app_info>& apps);

    dsn::error_code list_nodes(std::vector<node_info>& nodes);

    dsn::error_code list_app(const std::string& app_name, /*out*/ int32_t& app_id, /*out*/ std::vector< partition_configuration>& partitions);

    dsn::error_code list_node(const std::string& node, /*out*/ std::vector<replica_info>& replicas);

private:
    void end_meta_request(task_ptr callback, int retry_times, error_code err, dsn_message_t request, dsn_message_t resp);

    template<typename TRequest>
    dsn::task_ptr request_meta(
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
    {
        dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, 0);
        task_ptr task = ::dsn::rpc::create_rpc_response_task(msg, nullptr, [](error_code, dsn_message_t, dsn_message_t) {}, reply_hash);
        ::marshall(msg, *req);
        rpc::call(
            _meta_servers,
            msg,
            this,
            [this, task] (error_code err, dsn_message_t request, dsn_message_t response)
            {
                end_meta_request(std::move(task), 0, err, request, response);
            },
            0
         );
        return task;
    }

    template<typename TRequest>
    dsn::task_ptr request_node(
            rpc_address node,
            dsn_task_code_t code,
            std::shared_ptr<TRequest>& req,
            int timeout_milliseconds = 0,
            int reply_hash = 0
            )
    {
        dsn_message_t msg = dsn_msg_create_request(code, timeout_milliseconds, 0);
        task_ptr task = ::dsn::rpc::create_rpc_response_task(msg, nullptr, [](error_code, dsn_message_t, dsn_message_t) {}, reply_hash);
        ::marshall(msg, *req);
        rpc::call(
            node,
            msg,
            this,
            [this, task] (error_code err, dsn_message_t request, dsn_message_t response)
            {
                task->enqueue_rpc_response(err, response);
            },
            0
         );
        return task;
    }

private:
    dsn::rpc_address _meta_servers;
};

}} //namespace
