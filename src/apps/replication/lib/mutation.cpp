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
#include "mutation.h"


namespace dsn { namespace replication {

mutation::mutation()
{
    rpc_code = 0;
    _private0 = 0; 
    _not_logged = 1;
    _prepare_ts_ms = 0;
    _client_request = nullptr;
    _prepare_request = nullptr;
}

mutation::~mutation()
{
    if (_client_request != nullptr)
    {
        dsn_msg_release_ref(_client_request);
    }

    if (_prepare_request != nullptr)
    {
        dsn_msg_release_ref(_prepare_request);
    }
}

void mutation::move_from(mutation_ptr& old)
{
    data.updates = std::move(old->data.updates);
    rpc_code = old->rpc_code;
        
    _client_request = old->client_msg();
    if (_client_request)
    {
        old->_client_request = nullptr;
    }

    _prepare_request = old->prepare_msg();
    if (_prepare_request)
    {
        old->_prepare_request = nullptr;
    }
}

void mutation::set_client_request(dsn_task_code_t code, dsn_message_t request)
{
    dassert(_client_request == nullptr, "batch is not supported now");
    rpc_code = code;

    if (request != nullptr)
    {
        _client_request = request;
        dsn_msg_add_ref(request); // released on dctor

        void* ptr;
        size_t size;
        bool r = dsn_msg_read_next(request, &ptr, &size);
        dassert(r, "payload is not present");
        dsn_msg_read_commit(request, size);

        blob buffer((char*)ptr, 0, (int)size);
        data.updates.push_back(buffer);
    }    
}

/*static*/ mutation_ptr mutation::read_from(binary_reader& reader, dsn_message_t from)
{
    mutation_ptr mu(new mutation());
    unmarshall(reader, mu->data);
    unmarshall(reader, mu->rpc_code);

    // it is possible this is an emtpy mutation due to new primaries inserts empty mutations for holes
    dassert(mu->data.updates.size() == 1 || mu->rpc_code == RPC_REPLICATION_WRITE_EMPTY,
        "batch is not supported now");

    if (nullptr != from)
    {
        mu->_prepare_request = from;
        dsn_msg_add_ref(from); // released on dctor
    }
    
    sprintf(mu->_name, "%lld.%lld",
        static_cast<long long int>(mu->data.header.ballot),
        static_cast<long long int>(mu->data.header.decree));

    return mu;
}

void mutation::write_to(binary_writer& writer)
{
    marshall(writer, data);
    marshall(writer, rpc_code);
}

int mutation::clear_prepare_or_commit_tasks()
{
    int c = 0;
    for (auto it = _prepare_or_commit_tasks.begin(); it != _prepare_or_commit_tasks.end(); it++)
    {
        if (it->second->cancel(true))
        {
            c++;
        }        
    }

    _prepare_or_commit_tasks.clear();
    return c;
}

int mutation::clear_log_task()
{
    if (_log_task != nullptr && _log_task->cancel(true))
    {
        _log_task = nullptr;
        return 1;
    }
    return 0;
}

}} // namespace end
