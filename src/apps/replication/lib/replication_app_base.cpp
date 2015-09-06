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

#include "replica.h"
#include "mutation.h"
#include <dsn/internal/factory_store.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.2pc"

namespace dsn { namespace replication {

void register_replica_provider(replica_app_factory f, const char* name)
{
    ::dsn::utils::factory_store<replication_app_base>::register_factory(name, f, PROVIDER_TYPE_MAIN);
}

replication_app_base::replication_app_base(replica* replica)
{
    _physical_error = 0;
    _dir_data = replica->dir() + "/data";
    _dir_learn = replica->dir() + "/learn";

    _replica = replica;
    _last_committed_decree = _last_durable_decree = 0;

    if (!::dsn::utils::is_file_or_dir_exist(_dir_data.c_str()))
        mkdir_(_dir_data.c_str());

    if (!::dsn::utils::is_file_or_dir_exist(_dir_learn.c_str()))
        mkdir_(_dir_learn.c_str());
}

error_code replication_app_base::write_internal(mutation_ptr& mu)
{
    dassert (mu->data.header.decree == last_committed_decree() + 1, "");

    if (mu->rpc_code != RPC_REPLICATION_WRITE_EMPTY)
    {
        binary_reader reader(mu->data.updates[0]);
        dsn_message_t resp = (mu->client_msg() ? dsn_msg_create_response(mu->client_msg()) : nullptr);
        dispatch_rpc_call(mu->rpc_code, reader, resp);
    }
    else
    {
        on_empty_write();
    }

    if (_physical_error != 0)
    {
        derror("physical error %d occurs in replication local app %s", _physical_error, data_dir().c_str());
    }

    return _physical_error == 0 ? ERR_OK : ERR_LOCAL_APP_FAILURE;
}

void replication_app_base::dispatch_rpc_call(int code, binary_reader& reader, dsn_message_t response)
{
    auto it = _handlers.find(code);
    if (it != _handlers.end())
    {
        if (response)
        {
            int err = 0; // replication layer error
            ::marshall(response, err);            
        }
        it->second(reader, response);
    }
    else
    {
        dassert(false, "cannot find handler for rpc code %d in %s", code, data_dir().c_str());
    }
}

}} // end namespace
