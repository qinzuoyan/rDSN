#include <sofa/pbrpc/pbrpc.h>
#include <google/protobuf/io/printer.h>
#include <boost/lexical_cast.hpp>
#include <iostream>

#include <dsn/service_api_c.h>
#include <dsn/cpp/utils.h>
#include <dsn/dist/replication/client_ddl.h>

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

bool WebServlet(const sofa::pbrpc::HTTPRequest& request, sofa::pbrpc::HTTPResponse& response)
{
    google::protobuf::io::Printer printer(response.content.get(), '$');
    const std::map<std::string, std::string>& params = *request.query_params;
    auto find = params.find("meta");
    if (find == params.end()) {
        printer.Print("<h2>ERROR: no param \"meta\" specified</h2>");
        return true;
    }
    std::string meta = find->second;
    find = params.find("app");
    if (find == params.end()) {
        printer.Print("<h2>ERROR: no param \"app\" specified</h2>");
        return true;
    }
    std::string app = find->second;
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
            printer.Print("<h2>ERROR: invalid param \"meta\"</h2>");
            return true;
        }
        meta_servers.push_back(addr);
    }
    dsn::replication::replication_app_client_base::load_meta_servers(meta_servers);
    client_ddl client(meta_servers);
    int32_t app_id;
    std::vector< partition_configuration> partitions;
    dsn::error_code err = client.list_app(app, app_id, partitions);
    if(err != dsn::ERR_OK) {
        printer.Print("<h2>ERROR: get app info failed: $err$</h2>", "err", dsn_error_to_string(err));
        return true;
    }
    printer.Print("<b>AppName:</b> $app_name$<br>", "app_name", app);
    printer.Print("<b>AppID:</b> $app_id$<br>", "app_id", boost::lexical_cast<std::string>(app_id));
    printer.Print("<b>PartitionCount:</b> $pcount$<br>", "pcount", boost::lexical_cast<std::string>(partitions.size()));
    printer.Print("<b>Partitions:</b><br>");
    printer.Print("<table border=\"2\">");
    printer.Print("<tr><th align=\"right\">PartitionID</th><th align=\"right\">Ballot</th>"
                  "<th align=\"right\">Primary</th><th align=\"right\">Secondaries</th></tr>");
    for(int i = 0; i < partitions.size(); i++)
    {
        const dsn::replication::partition_configuration& p = partitions[i];
        printer.Print("<tr><td>$pid$</td>", "pid", boost::lexical_cast<std::string>(p.gpid.pidx));
        printer.Print("<td>$ballot$</td>", "ballot", boost::lexical_cast<std::string>(p.ballot));
        printer.Print("<td>$primary$</td>", "primary", p.primary.to_std_string());
        std::ostringstream oss;
        for(int j = 0; j < p.secondaries.size(); j++)
        {
            if(j != 0) oss << ", ";
            oss << p.secondaries[j].to_std_string();
        }
        printer.Print("<td>$secondaries$</td></tr>", "secondaries", oss.str());
    }
    printer.Print("</table>");
    return true;
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
    rpc_server.RegisterWebServlet("pegasus", servlet);

    // Start rpc server.
    std::string addr = std::string("0.0.0.0:") + argv[2];
    if (!rpc_server.Start(addr)) {
        SLOG(ERROR, "start server failed");
        return EXIT_FAILURE;
    }

    // Wait signal.
    rpc_server.Run();

    // Stop rpc server.
    rpc_server.Stop();

    return EXIT_SUCCESS;
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
