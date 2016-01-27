#include "replication_failure_detector.h"
#include "meta_server_failure_detector.h"
#include "replica_stub.h"
#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>
#include <vector>

#ifdef __TITLE__
#undef __TITLE__
#endif

#define __TITLE__ "fd.test"

using namespace dsn;

#define MPORT_START 30001
#define WPORT 40001
#define MCOUNT 3

DEFINE_TASK_CODE_RPC(RPC_MASTER_CONFIG, TASK_PRIORITY_COMMON, THREAD_POOL_FD)

struct config_master_message
{
    rpc_address master;
    bool is_register;
};

void marshall(rpc_write_stream& msg, const config_master_message &val)
{
    marshall(msg, val.master);
    marshall(msg, val.is_register);
}

void unmarshall(rpc_read_stream& msg, config_master_message &val)
{
    unmarshall(msg, val.master);
    unmarshall(msg, val.is_register);
}

volatile int started_apps = 0;
class worker_fd_test: public replication::replication_failure_detector
{
private:
    volatile bool _send_ping_switch;
    /* this function only triggerd once*/
    std::function<void (rpc_address addr)> _connected_cb;
    std::function<void (const std::vector<rpc_address>&)> _disconnected_cb;

protected:
    virtual void send_beacon(::dsn::rpc_address node, uint64_t time) override
    {
        if (_send_ping_switch)
            failure_detector::send_beacon(node, time);
        else
        {
            dinfo("ignore send beacon, to node[%s], time[%" PRId64 "]",
                  node.to_string(), time);
        }
    }

    virtual void on_master_disconnected(const std::vector<rpc_address> &nodes) override
    {
        if ( _disconnected_cb )
            _disconnected_cb(nodes);
    }

    virtual void on_master_connected(rpc_address node) override
    {
        if (_connected_cb)
            _connected_cb(node);
    }

public:
    worker_fd_test(replica_stub* stub, std::vector< dsn::rpc_address>& meta_servers):
        replication_failure_detector(stub, meta_servers)
    {
        _send_ping_switch = false;
    }
    void toggle_send_ping(bool toggle)
    {
        _send_ping_switch = toggle;
    }
    void when_connected(const std::function<void (rpc_address addr)>& func)
    {
        _connected_cb = func;
    }
    void when_disconnected(const std::function<void (const std::vector<rpc_address>& nodes)>& func)
    {
        _disconnected_cb = func;
    }
    void clear()
    {
        _connected_cb = {};
        _disconnected_cb = {};
    }
};

class master_fd_test: public meta_server_failure_detector
{
private:
    std::function<void (rpc_address addr)> _connected_cb;
    std::function<void (const std::vector<rpc_address>&)> _disconnected_cb;
    volatile bool _response_ping_switch;

protected:
    virtual void on_ping(const beacon_msg &beacon, ::dsn::rpc_replier<beacon_ack> &reply) override
    {
        if (_response_ping_switch)
            meta_server_failure_detector::on_ping(beacon, reply);
        else {
            dinfo("ignore on ping, beacon msg, time[%" PRId64 "], from[%s], to[%s]",
                  beacon.time,
                  beacon.from.to_string(),
                  beacon.to.to_string());
        }
    }

    virtual void on_worker_disconnected(const std::vector<rpc_address>& worker_list) override
    {
        if (_disconnected_cb)
            _disconnected_cb(worker_list);
    }
    virtual void on_worker_connected(rpc_address node) override
    {
        if (_connected_cb)
            _connected_cb(node);
    }
public:
    master_fd_test(): meta_server_failure_detector(rpc_address(), false)
    {
        _response_ping_switch = true;
    }
    void toggle_response_ping(bool toggle)
    {
        _response_ping_switch = toggle;
    }
    void when_connected(const std::function<void (rpc_address addr)>& func)
    {
        _connected_cb = func;
    }
    void when_disconnected(const std::function<void (const std::vector<rpc_address>& nodes)>& func)
    {
        _disconnected_cb = func;
    }
    void test_register_worker(rpc_address node)
    {
        zauto_lock l(failure_detector::_lock);
        register_worker(node);
    }
    void clear()
    {
        _connected_cb = {};
        _disconnected_cb = {};
    }
};

class test_worker: public service_app, public serverlet<test_worker>
{
public:
    test_worker(): serverlet("test_worker") {}
    error_code start(int argc, char** argv) override
    {
        std::vector<rpc_address> master_group;
        for (int i=0; i<3; ++i)
            master_group.push_back( rpc_address("localhost", MPORT_START+i) );
        _worker_fd = new worker_fd_test(nullptr, master_group);
        _worker_fd->start(1, 1, 4, 5);
        ++started_apps;

        register_rpc_handler(RPC_MASTER_CONFIG, "RPC_MASTER_CONFIG", &test_worker::on_master_config);
        return ERR_OK;
    }

    void stop(bool) override
    {

    }

    void on_master_config(const config_master_message& request, bool& response)
    {
        dinfo("master config: request:%s, type:%s", request.master.to_string(), request.is_register?"reg":"unreg");
        if ( request.is_register )
            _worker_fd->register_master( request.master );
        else
            _worker_fd->unregister_master( request.master );
        response = true;
    }

    worker_fd_test* fd() { return _worker_fd; }
private:
    worker_fd_test* _worker_fd;
};

class test_master: public service_app
{
public:
    error_code start(int, char **) override
    {
        _master_fd = new master_fd_test();
        _master_fd->start(1, 1, 4, 5);
        ++started_apps;

        return ERR_OK;
    }
    void stop(bool) override
    {

    }
    master_fd_test* fd() { return _master_fd; }
private:
    master_fd_test* _master_fd;
};

bool spin_wait_condition(const std::function<bool ()>& pred, int seconds)
{
    for (int i=0; i!=seconds; ++i) {
        if (pred())
            return true;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return pred();
}

void fd_test_init()
{
    dsn::register_app<test_worker>("worker");
    dsn::register_app<test_master>("master");
    srand(time(0));
}

bool get_worker_and_master(test_worker* &worker, std::vector<test_master*> &masters)
{
    started_apps = 0;
    bool ans = spin_wait_condition( [](){ return started_apps = MCOUNT+1; }, 30);
    if (!ans)
        return false;

    dsn_app_info* all_apps = new dsn_app_info[128];
    int total_apps_in_tst = dsn_get_all_apps(all_apps, 128);
    if (total_apps_in_tst>128) {
        derror("too much apps for this test case");
        return false;
    }

    worker = nullptr;
    masters.resize(MCOUNT, nullptr);

    for (int i=0; i!=total_apps_in_tst; ++i) {
        if ( strcmp(all_apps[i].type, "worker")==0 ) {
            if (worker!=nullptr)
                return false;
            worker = reinterpret_cast<test_worker*>(all_apps[i].app_context_ptr);
        }
        else if ( strcmp(all_apps[i].type, "master")==0 ) {
            int index = all_apps[i].index-1;
            if (index>=masters.size() || masters[index]!=nullptr)
                return false;
            masters[index] = reinterpret_cast<test_master*>(all_apps[i].app_context_ptr);
        }
    }

    for (test_master* m: masters)
        if (m==nullptr)
            return false;
    return true;
}

void master_group_set_leader(std::vector<test_master*>& master_group, int leader_index)
{
    rpc_address leader_addr("localhost", MPORT_START+leader_index);
    int i=0;
    for (test_master* &master: master_group)
    {
        master->fd()->set_leader_for_test(leader_addr, leader_index==i);
        i++;
    }
}

void worker_set_leader(test_worker* worker, int leader_contact)
{
    worker->fd()->set_leader_for_test( rpc_address("localhost", MPORT_START+leader_contact) );

    config_master_message msg = { rpc_address("localhost", MPORT_START+leader_contact), true };
    error_code err;
    bool response;
    std::tie(err, response) = rpc::call_wait<bool>(
        rpc_address("localhost", WPORT),
        dsn_task_code_t(RPC_MASTER_CONFIG),
        msg);
    ASSERT_TRUE(err == ERR_OK);
}

void clear(test_worker* worker, std::vector<test_master*> masters)
{
    rpc_address leader = dsn_group_get_leader(worker->fd()->get_servers().group_handle());

    config_master_message msg = { leader, false };
    error_code err;
    bool response;
    std::tie(err, response) = rpc::call_wait<bool>(
        rpc_address("localhost", WPORT),
        dsn_task_code_t(RPC_MASTER_CONFIG),
        msg);
    ASSERT_TRUE(err == ERR_OK);

    worker->fd()->toggle_send_ping(false);

    std::for_each(masters.begin(), masters.end(), [](test_master* mst) {
        mst->fd()->clear_workers();
        mst->fd()->toggle_response_ping(true);
    } );
}

void finish(test_worker* worker, test_master* master, int master_index)
{
    std::atomic_int wait_count;
    wait_count.store(2);
    worker->fd()->when_disconnected( [&wait_count, master_index](const std::vector<rpc_address>& addr_list) mutable {
        ASSERT_TRUE(addr_list.size() == 1);
        ASSERT_TRUE(addr_list[0].port() == MPORT_START+master_index );
        --wait_count;
    });

    master->fd()->when_disconnected( [&wait_count](const std::vector<rpc_address>& addr_list) mutable {
        ASSERT_TRUE(addr_list.size() == 1);
        ASSERT_TRUE(addr_list[0].port() == WPORT);
        --wait_count;
    });

    //we don't send any ping message now
    worker->fd()->toggle_send_ping(false);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count==0; }, 20));
    worker->fd()->clear();
    master->fd()->clear();
}

TEST(fd, dummy_connect_disconnect)
{
    test_worker* worker;
    std::vector<test_master*> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));

    clear(worker, masters);
    //set master with smallest index as the leader
    master_group_set_leader(masters, 0);
    // set the worker contact leader
    worker_set_leader(worker, 0);

    test_master* leader = masters[0];
    //simply wait for two connected
    std::atomic_int wait_count;
    wait_count.store(2);
    worker->fd()->when_connected( [&wait_count](rpc_address leader) mutable{
        ASSERT_TRUE(leader.port() == MPORT_START);
        --wait_count;
    });
    leader->fd()->when_connected( [&wait_count](rpc_address worker_addr) mutable {
        ASSERT_TRUE(worker_addr.port() == WPORT);
        --wait_count;
    });

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count==0; }, 20));

    finish(worker, leader, 0);
}

TEST(fd, master_redirect)
{
    test_worker* worker;
    std::vector<test_master*> masters;
    ASSERT_TRUE(get_worker_and_master(worker, masters));

    int index = masters.size()-1;

    clear(worker, masters);
    /* leader is the last master*/
    master_group_set_leader(masters, index);
    // we contact to 0
    worker_set_leader(worker, 0);

    test_master* leader = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);
    /* although we contact to the first master, but in the end we must connect to the right leader */
    worker->fd()->when_connected( [&wait_count, index](rpc_address leader) mutable{
        --wait_count;
    });
    leader->fd()->when_connected( [&wait_count](rpc_address worker_addr) mutable {
        ASSERT_TRUE(worker_addr.port() == WPORT);
        --wait_count;
    });

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition([&wait_count] { return wait_count==0; }, 20));
    //in the end, the worker will connect to the right master
    ASSERT_TRUE( spin_wait_condition([worker, index]{ return worker->fd()->current_server_contact().port()==MPORT_START+index; }, 20) );

    finish(worker, leader, index);
}

TEST(fd, switch_new_master_suddenly)
{
    test_worker* worker;
    std::vector<test_master*> masters;
    ASSERT_TRUE( get_worker_and_master(worker, masters) );

    clear(worker, masters);

    test_master* tst_master;
    int index = 0;

    master_group_set_leader(masters, index);
    //and now we contact to 1
    worker_set_leader(worker, 1);

    tst_master = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);

    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected( cb );
    tst_master->fd()->when_connected( cb );

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition( [&wait_count](){ return wait_count==0; }, 20));
    ASSERT_TRUE( worker->fd()->current_server_contact().port()==MPORT_START+index );

    worker->fd()->when_connected(nullptr);
    /* we select a new leader */
    index = masters.size()-1;
    tst_master = masters[index];
    /*
     * for perfect FD, the new master should assume the worker connected.
     * But first we test if the worker can connect to the new master.
     * So clear all the workers
     */
    tst_master->fd()->clear_workers();
    wait_count.store(1);
    tst_master->fd()->when_connected( [&wait_count](rpc_address addr) mutable {
        ASSERT_TRUE(addr.port() == WPORT);
        --wait_count;
    } );
    master_group_set_leader(masters, index);

    /* now we can worker the worker to connect to the new master */
    ASSERT_TRUE(spin_wait_condition( [&wait_count](){ return wait_count==0; }, 20 ));
    /* it may takes time for worker to switch to new master, but 20 seconds
     * is enough as in our setting, lease_period is 9 seconds. */
    ASSERT_TRUE(spin_wait_condition( [worker, index](){ return worker->fd()->current_server_contact().port()==MPORT_START+index; }, 20) );

    finish(worker, tst_master, index);
}

TEST(fd, old_master_died)
{
    test_worker* worker;
    std::vector<test_master*> masters;
    ASSERT_TRUE( get_worker_and_master(worker, masters) );
    clear(worker, masters);

    test_master* tst_master;
    int index = 0;
    master_group_set_leader(masters, index);
    //and now we contact to 0
    worker_set_leader(worker, 0);

    tst_master = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);

    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected( cb );
    tst_master->fd()->when_connected( cb );

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE( spin_wait_condition( [&wait_count]()->bool { return wait_count==0; }, 20) );
    ASSERT_TRUE( worker->fd()->current_server_contact().port()==MPORT_START+index );

    worker->fd()->when_connected( nullptr );
    tst_master->fd()->when_connected( nullptr );

    worker->fd()->when_disconnected( [](const std::vector<rpc_address>& masters_list){
        ASSERT_TRUE(masters_list.size()==1);
        dinfo("disconnect from master: %s", masters_list[0].to_string());
    } );

    /*first let's stop the old master*/
    tst_master->fd()->toggle_response_ping(false);
    /* then select a new one */
    index = masters.size()-1;
    tst_master = masters[index];

    /* only for test */
    tst_master->fd()->clear_workers();
    wait_count.store(1);

    tst_master->fd()->when_connected( [&wait_count](rpc_address addr) mutable {
        EXPECT_EQ(addr.port(), WPORT);
        --wait_count;
    } );
    master_group_set_leader(masters, index);

    /* now we can wait the worker to connect to the new master */
    ASSERT_TRUE(spin_wait_condition( [&wait_count](){
        return wait_count==0;
    }, 20 ));
    /* it may takes time for worker to switch to new master, but 20 seconds
     * is enough as in our setting, lease_period is 9 seconds. */
    ASSERT_TRUE(spin_wait_condition( [worker, index](){ return worker->fd()->current_server_contact().port()==MPORT_START+index; }, 20) );

    finish(worker, tst_master, index);
}

TEST(fd, worker_died_when_switch_master)
{
    test_worker* worker;
    std::vector<test_master*> masters;
    ASSERT_TRUE( get_worker_and_master(worker, masters) );
    clear(worker, masters);

    test_master* tst_master;
    int index = 0;
    master_group_set_leader(masters, index);
    //and now we contact to 0
    worker_set_leader(worker, 0);

    tst_master = masters[index];
    std::atomic_int wait_count;
    wait_count.store(2);

    auto cb = [&wait_count](rpc_address) mutable { --wait_count; };
    worker->fd()->when_connected( cb );
    tst_master->fd()->when_connected( cb );

    worker->fd()->toggle_send_ping(true);
    ASSERT_TRUE(spin_wait_condition( [&wait_count](){ return wait_count==0; }, 20));
    ASSERT_TRUE( worker->fd()->current_server_contact().port()==MPORT_START+index );

    worker->fd()->when_connected(nullptr);
    tst_master->fd()->when_connected(nullptr);

    /*first stop the old leader*/
    tst_master->fd()->toggle_response_ping(false);

    /*then select another leader*/
    index = masters.size()-1;
    tst_master = masters[index];

    wait_count.store(2);
    tst_master->fd()->when_disconnected( [&wait_count](const std::vector<rpc_address>& worker_list) mutable {
            ASSERT_TRUE(worker_list.size() == 1);
            ASSERT_TRUE(worker_list[0].port()==WPORT);
            wait_count--;
        } );
    worker->fd()->when_disconnected( [&wait_count](const std::vector<rpc_address>& master_list) mutable {
            ASSERT_TRUE(master_list.size() == 1);
            wait_count--;
        } );

    /* we assume the worker is alive */
    tst_master->fd()->test_register_worker( rpc_address("localhost", WPORT) );
    master_group_set_leader(masters, index);

    /* then stop the worker*/
    worker->fd()->toggle_send_ping(false);
    ASSERT_TRUE( spin_wait_condition( [&wait_count]{ return wait_count==0; }, 20 ) );
}
