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
# include <dsn/internal/global_config.h>
# include <thread>
# include <dsn/internal/task_spec.h>
# include <dsn/internal/network.h>
# include <dsn/internal/singleton_store.h>
# include "library_utils.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "ConfigFile"

namespace dsn {

static bool build_client_network_confs(
    const char* section, 
    __out_param network_client_configs& nss,
    network_client_configs* default_spec)
{
    nss.clear();

    const char* keys[128];
    int kcapacity = 128;
    int kcount = dsn_config_get_all_keys(section, keys, &kcapacity);
    dassert(kcount <= 128, "");

    for (int i = 0; i < kcapacity; i++)
    {
        std::string k = keys[i];
        if (k.length() <= strlen("network.client."))
            continue;

        if (k.substr(0, strlen("network.client.")) != std::string("network.client."))
            continue;

        auto k2 = k.substr(strlen("network.client."));
        if (rpc_channel::is_exist(k2.c_str()))
        {
            /*
            ;channel = network_provider_name,buffer_block_size
            network.client.RPC_CHANNEL_TCP = dsn::tools::asio_network_provider,65536
            network.client.RPC_CHANNEL_UDP = dsn::tools::asio_network_provider,65536
            */

            rpc_channel ch = rpc_channel::from_string(k2.c_str(), RPC_CHANNEL_TCP);

            // dsn::tools::asio_network_provider,65536
            std::list<std::string> vs;
            std::string v = dsn_config_get_value_string(section, k.c_str(), "", 
                "network channel configuration, e.g., dsn::tools::asio_network_provider,65536");
            utils::split_args(v.c_str(), vs, ',');

            if (vs.size() != 2)
            {
                printf("invalid client network specification '%s', should be '$network-factory,$msg-buffer-size'\n",
                    v.c_str()
                    );
                return false;
            }
            
            network_client_config ns;
            ns.factory_name = vs.begin()->c_str();
            ns.message_buffer_block_size = atoi(vs.rbegin()->c_str());

            if (ns.message_buffer_block_size == 0)
            {
                printf("invalid message buffer size specified: '%s'\n", vs.rbegin()->c_str());
                return false;
            }

            nss[ch] = ns;
        }
        else
        {
            printf("invalid rpc channel type: %s\n", k2.c_str());
            return false;
        }
    }

    if (default_spec)
    {
        for (auto& kv : *default_spec)
        {
            if (nss.find(kv.first) == nss.end())
            {
                nss[kv.first] = kv.second;
            }
        }
    }

    return true;
}


static bool build_server_network_confs(
    const char* section,
    __out_param network_server_configs& nss,
    network_server_configs* default_spec,
    const std::vector<int>& ports,
    bool is_template)
{
    nss.clear();

    const char* keys[128];
    int kcapacity = 128;
    int kcount = dsn_config_get_all_keys(section, keys, &kcapacity);
    dassert(kcount <= 128, "");

    for (int i = 0; i < kcapacity; i++)
    {
        std::string k = keys[i];
        if (k.length() <= strlen("network.server."))
            continue;

        if (k.substr(0, strlen("network.server.")) != std::string("network.server."))
            continue;

        auto k2 = k.substr(strlen("network.server."));
        std::list<std::string> ks;
        utils::split_args(k2.c_str(), ks, '.');
        if (ks.size() != 2)
        {
            printf("invalid network server config '%s', should be like 'network.server.12345.RPC_CHANNEL_TCP' instead\n", k.c_str());
            return false;
        }

        int port = atoi(ks.begin()->c_str());
        auto k3 = *ks.rbegin();

        if (is_template)
        {
            if (port != 0)
            {
                printf("invalid network server configuration '%s'\n", k.c_str());
                printf("port must be zero in [apps..default]\n");
                printf(" e.g., network.server.0.RPC_CHANNEL_TCP = NET_HDR_DSN, dsn::tools::asio_network_provider,65536\n");
                return false;
            }
        }
        else
        {
            if (std::find(ports.begin(), ports.end(), port) == ports.end())
            {
                continue;
            }
        }

        if (rpc_channel::is_exist(k3.c_str()))
        {
            /*            
            port = 0 for default setting in [apps..default]
            port.channel = header_format, network_provider_name,buffer_block_size
            network.server.port.RPC_CHANNEL_TCP = NET_HDR_DSN, dsn::tools::asio_network_provider,65536
            network.server.port.RPC_CHANNEL_UDP = NET_HDR_DSN, dsn::tools::asio_network_provider,65536
            */

            rpc_channel ch = rpc_channel::from_string(k3.c_str(), RPC_CHANNEL_TCP);
            network_server_config ns(port, ch);

            // NET_HDR_DSN, dsn::tools::asio_network_provider,65536
            std::list<std::string> vs;
            std::string v = dsn_config_get_value_string(section, k.c_str(), "", 
                "network channel configuration, e.g., NET_HDR_DSN, dsn::tools::asio_network_provider,65536");
            utils::split_args(v.c_str(), vs, ',');

            if (vs.size() != 3)
            {
                printf("invalid network specification '%s', should be '$message-format, $network-factory,$msg-buffer-size'\n",
                    v.c_str()
                    );
                return false;
            }

            if (!network_header_format::is_exist(vs.begin()->c_str()))
            {
                printf("invalid network specification, unkown message header format '%s'\n",
                    vs.begin()->c_str()
                    );
                return false;
            }

            ns.hdr_format = network_header_format(vs.begin()->c_str());
            ns.factory_name = *(++vs.begin());
            ns.message_buffer_block_size = atoi(vs.rbegin()->c_str());

            if (ns.message_buffer_block_size == 0)
            {
                printf("invalid message buffer size specified: '%s'\n", vs.rbegin()->c_str());
                return false;
            }

            nss[ns] = ns;
        }
        else
        {
            printf("invalid rpc channel type: %s\n", k3.c_str());
            return false;
        }
    }

    if (default_spec)
    {
        for (auto& kv : *default_spec)
        {
            network_server_config cs = kv.second;
            for (auto& port : ports)
            {
                cs.port = port;
                if (nss.find(cs) == nss.end())
                {
                    nss[cs] = cs;
                }
            }

            if (is_template)
            {
                cs.port = 0;
                if (nss.find(cs) == nss.end())
                {
                    nss[cs] = cs;
                }
            }
        }
    }

    return true;
}

service_app_spec::service_app_spec(const service_app_spec& r)
{
    index = r.index;
    id = r.id;
    config_section = r.config_section;
    name = r.name;
    role = r.role;
    type = r.type;
    arguments = r.arguments;
    ports = r.ports;
    pools = r.pools;
    delay_seconds = r.delay_seconds;
    run = r.run;
    dmodule = r.dmodule;
    network_client_confs = r.network_client_confs;
    network_server_confs = r.network_server_confs;
}

bool service_app_spec::init(
    const char* section, 
    const char* r, 
    service_app_spec* default_value,
    network_client_configs* default_client_nets,
    network_server_configs* default_server_nets
    )
{
    id = 0;
    index = 0;
    name = r;
    config_section = std::string(section);

    if (!read_config(section, *this, default_value))
        return false;

    std::sort(ports.begin(), ports.end());

    if (!build_client_network_confs(
        section,
        this->network_client_confs,
        default_value ? &default_value->network_client_confs : default_client_nets
        ))
        return false;

    if (!build_server_network_confs(
        section,
        this->network_server_confs,
        default_value ? &default_value->network_server_confs : default_server_nets,
        ports,
        default_value == nullptr
        ))
        return false;

    return true;
}


network_server_config::network_server_config(const network_server_config& r)
: channel(r.channel), hdr_format(r.hdr_format)
{
    port = r.port;
    factory_name = r.factory_name;
    message_buffer_block_size = r.message_buffer_block_size;
}

network_server_config::network_server_config(int p, rpc_channel c)
    : channel(c), hdr_format(NET_HDR_DSN)
{
    port = p;
    factory_name = "dsn::tools::asio_network_provider";
    message_buffer_block_size = 65536;
}

bool network_server_config::operator < (const network_server_config& r) const
{
    return port < r.port || (port == r.port && channel < r.channel);
}

bool service_spec::init()
{
    // init common spec
    if (!read_config("core", *this, nullptr))
        return false;

    // init thread pools
    if (!threadpool_spec::init(threadpool_specs))
        return false;

    // init task specs
    if (!task_spec::init())
        return false;
    
    return true;
}

bool service_spec::init_app_specs()
{
    // init service apps
    service_app_spec default_app;
    if (!default_app.init("apps..default", ".default", nullptr,
        &this->network_default_client_cfs,
        &this->network_default_server_cfs
        ))
        return false;

    std::vector<std::string> allSectionNames;
    config->get_all_sections(allSectionNames);
    
    int app_id = 0;
    for (auto it = allSectionNames.begin(); it != allSectionNames.end(); it++)
    {
        if (it->substr(0, strlen("apps.")) == std::string("apps.") && *it != std::string("apps..default"))
        {
            service_app_spec app;
            if (!app.init((*it).c_str(), it->substr(5).c_str(), &default_app))
                return false;

            // load dynamic module where app role is defined
            if (app.dmodule.length() > 0)
            {
                if (!::dsn::utils::load_dynamic_library(app.dmodule.c_str()))
                {
                    return false;
                }   
            }

            auto& store = ::dsn::utils::singleton_store<std::string, ::dsn::service_app_role>::instance();
            ::dsn::service_app_role role;
            if (!store.get(app.type, role))
            {
                printf("service type '%s' not registered\n", app.type.c_str());
                return false;
            }

            app.role = role;

            auto ports = app.ports;   
            auto nsc = app.network_server_confs;
            auto gap = ports.size() > 0 ? (*ports.rbegin() + 1 - *ports.begin()) : 0;            
            std::string name = app.name;
            for (int i = 1; i <= app.count; i++)
            {
                char buf[16];
                sprintf(buf, "%u", i);
                app.name = (app.count > 1 ? (name + buf) : name);
                app.id = ++app_id;
                app.index = i;

                // add app
                app_specs.push_back(app);

                // for next instance
                app.ports.clear();
                for (auto& p : ports)
                {
                    app.ports.push_back(p + i * gap);
                }

                app.network_server_confs.clear();
                for (auto sc : nsc)
                {
                    sc.second.port += i * gap;
                    app.network_server_confs[sc.second] = sc.second;
                }
            }
        }
    }

    return true;
}

} // end namespace dsn
