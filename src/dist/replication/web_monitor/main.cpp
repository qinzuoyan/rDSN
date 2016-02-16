#include <sofa/pbrpc/pbrpc.h>
#include <google/protobuf/io/printer.h>
#include <boost/lexical_cast.hpp>
#include <iostream>
#include <algorithm>

#include <dsn/service_api_c.h>
#include <dsn/cpp/utils.h>
#include "monitor_client.h"

using namespace dsn::replication;

extern void dsn_core_init();

int init_environment(char* exe, char* config_file)
{
    dsn_core_init();

    //use config file to run
    char* argv[] = {exe, config_file};

    dsn_run(2, argv, false);
    return 0;
}

bool ListApps(const sofa::pbrpc::HTTPRequest& request, sofa::pbrpc::HTTPResponse& response,
              const std::string& meta, const std::vector<dsn::rpc_address>& meta_servers)
{
    const std::map<std::string, std::string>& params = *request.query_params;
    google::protobuf::io::Printer printer(response.content.get(), '$');
    monitor_client client(meta_servers);
    dsn::error_code err;

    std::vector<node_info> nodes;
    err = client.list_nodes(nodes);
    if(err != dsn::ERR_OK) {
        printer.Print("<h2>ERROR: get nodes failed: $err$</h2>", "err", dsn_error_to_string(err));
        return true;
    }
    std::sort(nodes.begin(), nodes.end(), [](const node_info& l, const node_info& r){return l.address < r.address;});

    std::vector<app_info> apps;
    err = client.list_apps(apps);
    if(err != dsn::ERR_OK) {
        printer.Print("<h2>ERROR: get apps failed: $err$</h2>", "err", dsn_error_to_string(err));
        return true;
    }
    std::sort(apps.begin(), apps.end(), [](const app_info& l, const app_info& r){return l.app_id < r.app_id;});

    printer.Print("<h2>Cluster at <a href=\"/pegasus?meta=$meta$\">$meta$</a>&emsp;&emsp;<a href=\"/pegasus\">&gt;&gt;&gt;&gt;Change Cluster</a></h2>", "meta", meta);

    printer.Print("<b>PrimaryMetaServer:</b> $primary_meta$<br/>", "primary_meta", client.primary_meta_server().to_string());

    printer.Print("<h3>ReplicaServers (count=$count$)</h3><hr/>", "count", boost::lexical_cast<std::string>(nodes.size()));
    printer.Print("<table border=\"2\">");
    printer.Print("<tr><th align=\"right\">Address</th><th align=\"right\">Status</th></tr>");
    for(int i = 0; i < nodes.size(); i++)
    {
        const dsn::replication::node_info& node = nodes[i];
        std::string node_addr = node.address.to_string();
        printer.Print("<tr><td><a href=\"/pegasus?meta=$meta$&node=$node_addr$\">$node_addr$</a></td>",
                      "meta", meta, "node_addr", node_addr);
        printer.Print("<td>$status$</td></tr>", "status", enum_to_string(node.status));
    }
    printer.Print("</table>");

    printer.Print("<h3>Apps (count=$count$)</h3><hr/>", "count",  boost::lexical_cast<std::string>(apps.size()));
    printer.Print("<table border=\"2\">");
    printer.Print("<tr><th align=\"right\">Name</th><th align=\"right\">AppID</th>"
                  "<th align=\"right\">Type</th><th align=\"right\">PartitionCount</th>"
                  "<th align=\"right\">Status</th></tr>");
    for(int i = 0; i < apps.size(); i++)
    {
        const dsn::replication::app_info& app = apps[i];
        printer.Print("<tr><td><a href=\"/pegasus?meta=$meta$&app=$name$\">$name$</a></td>",
                      "meta", meta, "name", app.app_name);
        printer.Print("<td>$id$</td>", "id", boost::lexical_cast<std::string>(app.app_id));
        printer.Print("<td>$type$</td>", "type", app.app_type);
        printer.Print("<td>$pcount$</td>", "pcount", boost::lexical_cast<std::string>(app.partition_count));
        printer.Print("<td>$status$</td></tr>", "status", enum_to_string(app.status));
    }
    printer.Print("</table>");

    return true;
}

bool ListApp(const sofa::pbrpc::HTTPRequest& request, sofa::pbrpc::HTTPResponse& response,
             const std::string& meta, const std::vector<dsn::rpc_address>& meta_servers,
             const std::string& app_name)
{
    const std::map<std::string, std::string>& params = *request.query_params;
    google::protobuf::io::Printer printer(response.content.get(), '$');
    monitor_client client(meta_servers);

    int32_t app_id;
    std::vector<partition_configuration> partitions;
    dsn::error_code err = client.list_app(app_name, app_id, partitions);
    if(err != dsn::ERR_OK) {
        printer.Print("<h2>ERROR: get app info failed: $err$</h2>", "err", dsn_error_to_string(err));
        return true;
    }
    printer.Print("<b>MetaServers:</b> <a href=\"/pegasus?meta=$meta$\">$meta$</a><br/>", "meta", meta);
    printer.Print("<b>AppName:</b> <a href=\"/pegasus?meta=$meta$&app=$app_name$\">$app_name$</a><br/>",
                  "meta", meta, "app_name", app_name);
    printer.Print("<b>AppID:</b> $app_id$<br/>", "app_id", boost::lexical_cast<std::string>(app_id));
    printer.Print("<b>PartitionCount:</b> $pcount$<br/>", "pcount", boost::lexical_cast<std::string>(partitions.size()));
    printer.Print("<b>Partitions:</b><br/>");
    printer.Print("<table border=\"2\">");
    printer.Print("<tr><th align=\"right\">GPID</th><th align=\"right\">Ballot</th>"
                  "<th align=\"right\">Primary</th><th align=\"right\">Secondaries</th></tr>");
    for(int i = 0; i < partitions.size(); i++)
    {
        const dsn::replication::partition_configuration& p = partitions[i];
        printer.Print("<tr><td>$app_id$.$pid$</td>",
                      "app_id", boost::lexical_cast<std::string>(p.gpid.app_id),
                      "pid", boost::lexical_cast<std::string>(p.gpid.pidx));
        printer.Print("<td>$ballot$</td>", "ballot", boost::lexical_cast<std::string>(p.ballot));
        if (p.primary.is_invalid())
        {
            printer.Print("<td>$primary$</td>", "primary", p.primary.to_std_string());
        }
        else
        {
            printer.Print("<td><a href=\"/pegasus?meta=$meta$&node=$primary$\">$primary$</a></td>",
                          "meta", meta, "primary", p.primary.to_std_string());
        }
        std::ostringstream oss;
        for(int j = 0; j < p.secondaries.size(); j++)
        {
            if(j != 0) oss << ", ";
            std::string node = p.secondaries[j].to_std_string();
            oss << "<a href=\"/pegasus?meta=" << meta << "&node=" << node << "\">" << node << "</a>";
        }
        printer.Print("<td>$secondaries$</td></tr>", "secondaries", oss.str());
    }
    printer.Print("</table>");

    return true;
}

bool ListNode(const sofa::pbrpc::HTTPRequest& request, sofa::pbrpc::HTTPResponse& response,
             const std::string& meta, const std::vector<dsn::rpc_address>& meta_servers,
             const std::string& node)
{
    const std::map<std::string, std::string>& params = *request.query_params;
    google::protobuf::io::Printer printer(response.content.get(), '$');
    monitor_client client(meta_servers);

    std::vector<replica_info> replicas;
    dsn::error_code err = client.list_node(node, replicas);
    if(err != dsn::ERR_OK) {
        printer.Print("<h2>ERROR: get node info failed: $err$</h2>", "err", dsn_error_to_string(err));
        return true;
    }

    std::map<partition_status, int> status_count;
    status_count[PS_INACTIVE] = 0;
    status_count[PS_ERROR] = 0;
    status_count[PS_PRIMARY] = 0;
    status_count[PS_SECONDARY] = 0;
    status_count[PS_POTENTIAL_SECONDARY] = 0;
    for (auto& r : replicas)
    {
        status_count[r.status] += 1;
    }

    printer.Print("<b>MetaServers:</b> <a href=\"/pegasus?meta=$meta$\">$meta$</a><br/>", "meta", meta);
    printer.Print("<b>NodeAddress:</b> <a href=\"/pegasus?meta=$meta$&node=$node$\">$node$</a><br/>",
                  "meta", meta, "node", node);

    printer.Print("<b>ReplicaCount:</b> $count$<br/>", "count", boost::lexical_cast<std::string>(replicas.size()));
    printer.Print("<table border=\"2\">");
    printer.Print("<tr><th align=\"right\">Status</th><th align=\"right\">Count</th></tr>");
    for (auto& i : status_count)
    {
        printer.Print("<tr><td>$status$</td>", "status", enum_to_string(i.first));
        printer.Print("<td>$count$</td></tr>", "count", boost::lexical_cast<std::string>(i.second));
    }
    printer.Print("</table>");

    printer.Print("<b>Replicas:</b><br/>");
    printer.Print("<table border=\"2\">");
    printer.Print("<tr><th align=\"right\">GPID</th><th align=\"right\">Ballot</th>"
                  "<th align=\"right\">Status</th><th align=\"right\">LastCommittedDecree</th>"
                  "<th align=\"right\">LastPreparedDecree</th><th align=\"right\">LastDurableDecree</th></tr>");
    for (auto& r : replicas)
    {
        printer.Print("<tr><td>$app_id$.$pid$</td>",
                      "app_id", boost::lexical_cast<std::string>(r.gpid.app_id),
                      "pid", boost::lexical_cast<std::string>(r.gpid.pidx));
        printer.Print("<td>$ballot$</td>", "ballot", boost::lexical_cast<std::string>(r.ballot));
        printer.Print("<td>$status$</td>", "status", enum_to_string(r.status));
        printer.Print("<td>$decree$</td>", "decree", boost::lexical_cast<std::string>(r.last_committed_decree));
        printer.Print("<td>$decree$</td>", "decree", boost::lexical_cast<std::string>(r.last_prepared_decree));
        printer.Print("<td>$decree$</td></tr>", "decree", boost::lexical_cast<std::string>(r.last_durable_decree));
    }
    printer.Print("</table>");

    return true;
}

bool WebServlet(const sofa::pbrpc::HTTPRequest& request, sofa::pbrpc::HTTPResponse& response)
{
    google::protobuf::io::Printer printer(response.content.get(), '$');
    const std::map<std::string, std::string>& params = *request.query_params;

    auto find = params.find("meta");
    if (find == params.end()) {
        printer.Print("<form action=\"/pegasus\" method=\"get\">");
        printer.Print("<b>MetaServers:</b> <input type=\"text\" name=\"meta\" size=\"100\" value=\"10.235.114.26:34601,10.235.114.26:34602,10.235.114.26:34603\"/><br/>");
        printer.Print("<input type=\"submit\">");
        printer.Print("</form>");
        return true;
    }
    std::string meta = find->second;
    std::vector<std::string> meta_names;
    dsn::utils::split_args(meta.c_str(), meta_names, ',');
    if (meta_names.empty()) {
        printer.Print("<h2>ERROR: invalid param \"meta\"</h2>");
        return true;
    }
    std::vector<dsn::rpc_address> meta_servers;
    for (auto& name : meta_names) {
        dsn::rpc_address addr;
        if (!addr.from_string_ipv4(name.c_str())) {
            printer.Print("<h2>ERROR: invalid param \"meta\": bad addr: $addr$</h2>", "addr", name);
            return true;
        }
        meta_servers.push_back(addr);
    }

    find = params.find("app");
    if (find != params.end()) {
        return ListApp(request, response, meta, meta_servers, find->second);
    }
    find = params.find("node");
    if (find != params.end()) {
        return ListNode(request, response, meta, meta_servers, find->second);
    }
    return ListApps(request, response, meta, meta_servers);
}

int main(int argc, char** argv)
{
    SOFA_PBRPC_SET_LOG_LEVEL(INFO);

    if(argc < 3) {
        SLOG(ERROR, "Usage: %s <config-file> <port>", argv[0]);
        return EXIT_FAILURE;
    }

    if(init_environment(argv[0], argv[1]) < 0) {
        SLOG(ERROR, "init environment failed");
        return EXIT_FAILURE;
    }

    // Define an rpc server.
    sofa::pbrpc::RpcServerOptions options;
    sofa::pbrpc::RpcServer rpc_server(options);

    sofa::pbrpc::Servlet servlet = sofa::pbrpc::NewPermanentExtClosure(&WebServlet);
    rpc_server.RegisterWebServlet("/pegasus", servlet);

    // Start rpc server.
    std::string addr = std::string("0.0.0.0:") + argv[2];
    if (!rpc_server.Start(addr)) {
        SLOG(ERROR, "start server failed");
        dsn_exit(EXIT_FAILURE);
    }

    // Wait signal.
    rpc_server.Run();

    // Stop rpc server.
    rpc_server.Stop();

    dsn_exit(EXIT_SUCCESS);
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
