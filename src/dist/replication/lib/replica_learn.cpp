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
 *     replication learning process
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include <dsn/internal/factory_store.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.learn"

namespace dsn { namespace replication {

void replica::init_learn(uint64_t signature)
{
    check_hashed_access();

    if (status() != PS_POTENTIAL_SECONDARY)
    {
        dwarn("%s: state is not potential secondary but %s, skip learning with signature[%016llx]",
            name(), enum_to_string(status()), signature
            );
        return;
    }

    if (signature == invalid_signature)
    {
        dwarn("%s: invalid learning signature, skip",
            name()
            );
        return;
    }
        
    // at most one learning task running
    if (_potential_secondary_states.learning_round_is_running)
    {
        dwarn("%s: previous learning is still running, skip learning with signature [%016llx]",
            name(), signature
            );
        return;
    }   

    // learn timeout or primary change, the (new) primary starts another round of learning process
    // be cautious: primary should not issue signatures frequently to avoid learning abort
    if (signature != _potential_secondary_states.learning_signature)
    {
        if (!_potential_secondary_states.cleanup(false))
        {
            dwarn("%s: previous learning with signature[%016llx] is still in-process, skip init new learning with signature [%016llx]",
                name(), _potential_secondary_states.learning_signature, signature
                );
            return;
        }   

        _potential_secondary_states.learning_signature = signature;
        _potential_secondary_states.learning_start_ts_ns = dsn_now_ns();
        _potential_secondary_states.learning_status = LearningWithoutPrepare;
        _prepare_list->reset(_app->last_committed_decree());
    }
    else
    {
        switch (_potential_secondary_states.learning_status)
        {
        // any failues in the process
        case LearningFailed:
            break;

        // learned state (app state) completed
        case LearningWithPrepare:
            dassert(_app->last_durable_decree() + 1 >= _potential_secondary_states.learning_start_prepare_decree,
                "learned state is incomplete");
            {
                // check missing state due to _app->flush to checkpoint the learned state
                auto c = _prepare_list->last_committed_decree();

                // TODO(qinzuoyan): to test the following lines
                // missing commits
                if (c > _app->last_committed_decree())
                {
                    // missed ones are covered by prepare list
                    if (_app->last_committed_decree() > _prepare_list->min_decree())
                    {
                        for (auto d = _app->last_committed_decree() + 1; d <= c; d++)
                        {
                            auto mu = _prepare_list->get_mutation_by_decree(d);
                            dassert(nullptr != mu, "");
                            auto err = _app->write_internal(mu);
                            if (ERR_OK != err)
                            {
                                handle_learning_error(err);
                                return;
                            }
                        }
                    }

                    // missed ones need to be loaded via private logs
                    else
                    {
                        _potential_secondary_states.learning_round_is_running = true;
                        _potential_secondary_states.catchup_with_private_log_task = tasking::create_task(
                            LPC_CATCHUP_WITH_PRIVATE_LOGS,
                            this,
                            [this]() { this->catch_up_with_private_logs(PS_POTENTIAL_SECONDARY); },
                            gpid_to_hash(get_gpid())
                            );
                        _potential_secondary_states.catchup_with_private_log_task->enqueue();

                        return; // incomplete
                    }
                }

                // no missing commits
                else
                {
                }

                // convert to success if app state and prepare list is connected
                _potential_secondary_states.learning_status = LearningSucceeded;
                // fall through to success
            }

        // app state and prepare list is connected
        case LearningSucceeded:
            {
                check_state_completeness();
                notify_learn_completion();
                return;
            }
            break;
        case LearningWithoutPrepare:
            break;
        default:
            dassert (false, "");
        }
    }
        
    _potential_secondary_states.learning_round_is_running = true;

    learn_request request;
    request.gpid = get_gpid();
    request.last_committed_decree_in_app = _app->last_committed_decree();
    request.last_committed_decree_in_prepare_list = _prepare_list->last_committed_decree();
    request.learner = _stub->_primary_address;
    request.signature = _potential_secondary_states.learning_signature;
    _app->prepare_learn_request(request.app_specific_learn_request);

    ddebug(
        "%s: init_learn[%016llx]: learnee = %s, learn duration = %" PRIu64 " ms, local_committed_decree = %" PRId64 ", "
        "app_committed_decree = %" PRId64 ", app_durable_decree = %" PRId64 ", current_learning_status = %s",
        name(), request.signature, _config.primary.to_string(),
        _potential_secondary_states.duration_ms(),
        last_committed_decree(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        enum_to_string(_potential_secondary_states.learning_status)
        );

    _potential_secondary_states.learning_task = 
        rpc::create_message(RPC_LEARN, request, gpid_to_hash(get_gpid()))
        .call(_config.primary, this, [this, req_cap = std::move(request)](error_code err, learn_response&& resp) mutable
        {
            on_learn_reply(err, std::move(req_cap), std::move(resp));
        }
        );
}

void replica::on_learn(dsn_message_t msg, const learn_request& request)
{
    check_hashed_access();
    
    learn_response response;
    if (PS_PRIMARY != status())
    {
        response.err = (PS_INACTIVE == status() && _inactive_is_transient) ? ERR_INACTIVE_STATE : ERR_INVALID_STATE;
        reply(msg, response);
        return;
    }

    // but just set state to PS_POTENTIAL_SECONDARY
    _primary_states.get_replica_config(PS_POTENTIAL_SECONDARY, response.config);

    auto it = _primary_states.learners.find(request.learner);
    if (it == _primary_states.learners.end())
    {
        response.config.status = PS_INACTIVE;
        response.err = ERR_OBJECT_NOT_FOUND;
        reply(msg, response);
        return;
    }
    else if (it->second.signature != request.signature)
    {
        response.config.learner_signature = it->second.signature;
        response.err = ERR_WRONG_CHECKSUM; // means invalid signature
        reply(msg, response);
        return;
    }

    // prepare learn_start_decree
    decree local_committed_decree = last_committed_decree();
    
    // TODO: learner machine has been down for a long time, and DDD MUST happened before
    // which leads to state lost. Now the lost state is back, what shall we do?
    if (request.last_committed_decree_in_app > last_prepared_decree())
    {
        derror(
            "%s: on_learn[%016llx]: learner = %s, learner state is newer than learnee, "
            "learner_app_committed_decree = %" PRId64 ", local_committed_decree = %" PRId64 ", learn from scratch",
            name(), request.signature, request.learner.to_string(),
            request.last_committed_decree_in_app, local_committed_decree
            );

        *(decree*)&request.last_committed_decree_in_app = 0;
    }

    // mutations are previously committed already on learner (old primary)
    // TODO(qinzuoyan): this case will never occur?
    else if (request.last_committed_decree_in_app > local_committed_decree)
    {
        derror(
            "%s: on_learn[%016llx]: learner = %s, learner's last_committed_decree_in_app is newer than learnee, "
            "learner_app_committed_decree = %" PRId64 ", local_committed_decree = %" PRId64 ", commit local hard",
            name(), request.signature, request.learner.to_string(),
            request.last_committed_decree_in_app, local_committed_decree
            );

        _prepare_list->commit(request.last_committed_decree_in_app, COMMIT_TO_DECREE_HARD);
        local_committed_decree = last_committed_decree();
    }

    dassert(request.last_committed_decree_in_app <= local_committed_decree, "");

    decree learn_start_decree = request.last_committed_decree_in_app + 1;
    dassert(learn_start_decree <= local_committed_decree + 1, "");
    bool delayed_replay_prepare_list = false;

    ddebug(
        "%s: on_learn[%016llx]: learner = %s, remote_committed_decree = %" PRId64 ", "
        "remote_app_committed_decree = %" PRId64 ", local_committed_decree = %" PRId64 ", "
        "app_committed_decree = %" PRId64 ", app_durable_decree = %" PRId64 ", "
        "prepare_min_decree = %" PRId64 ", prepare_list_count = %d, learn_start_decree = %" PRId64,
        name(), request.signature, request.learner.to_string(),
        request.last_committed_decree_in_prepare_list,
        request.last_committed_decree_in_app,
        local_committed_decree,
        _app->last_committed_decree(), 
        _app->last_durable_decree(),
        _prepare_list->min_decree(),
        _prepare_list->count(),
        learn_start_decree
        );

    response.address = _stub->_primary_address;
    response.prepare_start_decree = invalid_decree;
    response.last_committed_decree = local_committed_decree;
    response.err = ERR_OK; 

    // set prepare_start_decree when to-be-learn state is covered by prepare list,
    // note min_decree can be NOT present in prepare list when list.count == 0
    if (learn_start_decree > _prepare_list->min_decree() 
       || (learn_start_decree == _prepare_list->min_decree() 
           && _prepare_list->count() > 0)
       )
    {
        if (it->second.prepare_start_decree == invalid_decree)
        {
            // start from (last_committed_decree + 1)
            it->second.prepare_start_decree = local_committed_decree + 1;

            cleanup_preparing_mutations(false);
            
            // the replayed prepare msg needs to be AFTER the learning response msg
            delayed_replay_prepare_list = true;
            
            ddebug(
                "%s: on_learn[%016llx]: learner = %s, set prepare_start_decree = %" PRId64,
                name(), request.signature, request.learner.to_string(),
                local_committed_decree + 1
            );
        }

        response.prepare_start_decree = it->second.prepare_start_decree;
    }
    else
    {
        it->second.prepare_start_decree = invalid_decree;
    }

    // only learn mutation cache in range of [learn_start_decree, prepare_start_decree),
    // in this case, the state on the PS should be contiguous (+ to-be-sent prepare list)
    if (response.prepare_start_decree != invalid_decree)
    {
        binary_writer writer;
        int count = 0;
        for (decree d = learn_start_decree; d < response.prepare_start_decree; d++)
        {
            auto mu = _prepare_list->get_mutation_by_decree(d);
            dassert(mu != nullptr, "");
            mu->write_to(writer);
            count++;
        }
        response.type = LT_CACHE;
        response.state.meta.push_back(writer.get_buffer());
        ddebug(
            "%s: on_learn[%016llx]: learner = %s, learn mutation cache succeed, "
            "learn_start_decree = %" PRId64 ", prepare_start_decree = %" PRId64 ", "
            "learn_mutation_count = %d, learn_data_size = %d",
            name(), request.signature, request.learner.to_string(),
            learn_start_decree, response.prepare_start_decree,
            count, response.state.meta[0].length()
            );
    }

    // learn delta state or checkpoint
    // in this case, the state on the PS is still incomplete
    else if (_app->is_delta_state_learning_supported() 
        || learn_start_decree <= _app->last_durable_decree())
    {
        ::dsn::error_code err = _app->get_checkpoint(
            learn_start_decree, 
            request.app_specific_learn_request, 
            response.state
            );

        if (err != ERR_OK)
        {
            response.err = ERR_GET_LEARN_STATE_FAILED;
            derror(
                "%s: on_learn[%016llx]: learner = %s, get app checkpoint failed, error = %s",
                name(), request.signature, request.learner.to_string(), err.to_string()
                );
        }
        else
        {
            response.type = LT_APP;
            response.base_local_dir = _app->data_dir();
            ddebug(
                "%s: on_learn[%016llx]: learner = %s, get app learn state succeed, base_local_dir = %s, learn_file_count = %u",
                name(), request.signature, request.learner.to_string(),
                response.base_local_dir.c_str(), static_cast<uint32_t>(response.state.files.size())
                );
        }
    }

    // learn private replication logs
    // in this case, the state on the PS is still incomplete
    else
    {
        dassert(_private_log != nullptr, 
            "log_enable_private_prepare must be enabled for %s when the replicated app "
            "does not support delta state learning",
            name()
            );

        _private_log->get_learn_state(get_gpid(), learn_start_decree, response.state);
        response.type = LT_LOG;
        response.base_local_dir = _private_log->dir();
        ddebug(
            "%s: on_learn[%016llx]: learner = %s, learn private logs succeed, base_local_dir = %s, learn_file_count = %u",
            name(), request.signature, request.learner.to_string(),
            response.base_local_dir.c_str(), static_cast<uint32_t>(response.state.files.size())
            );
    }

    for (auto& file : response.state.files)
    {
        file = file.substr(response.base_local_dir.length() + 1);
    }

    reply(msg, response);

    // the replayed prepare msg needs to be AFTER the learning response msg
    if (delayed_replay_prepare_list)
    {
        replay_prepare_list();
    }   
}

void replica::on_learn_reply(
    error_code err, 
    learn_request&& req,
    learn_response&& resp
    )
{
    check_hashed_access();

    dassert(PS_POTENTIAL_SECONDARY == status(), "");
    dassert(req.signature == _potential_secondary_states.learning_signature, "");

    if (err != ERR_OK)
    {
        handle_learning_error(err);
        return;
    }

    ddebug(
        "%s: on_learn_reply[%016llx]: learnee = %s, learn duration = %" PRIu64 " ms, response_err = %s, remote_committed_decree = %" PRId64 ", "
        "prepare_start_decree = %" PRId64 ", learn_type = %s, learn_file_count = %u, current_learning_status = %s",
        name(), req.signature, resp.config.primary.to_string(),
        _potential_secondary_states.duration_ms(),
        resp.err.to_string(), 
        resp.last_committed_decree, 
        resp.prepare_start_decree,
        enum_to_string(resp.type),
        static_cast<uint32_t>(resp.state.files.size()),
        enum_to_string(_potential_secondary_states.learning_status)
        );

    if (resp.err != ERR_OK)
    {
        if (resp.err == ERR_INACTIVE_STATE)
        {
            dwarn(
                "%s: on_learn_reply[%016llx]: learnee = %s, learnee is updating ballot, delay to start another round of learning",
                name(), req.signature, resp.config.primary.to_string()
                );
            _potential_secondary_states.learning_round_is_running = false;
            _potential_secondary_states.delay_learning_task = tasking::create_task(
                LPC_DELAY_LEARN,
                this,
                std::bind(&replica::init_learn, this, req.signature),
                gpid_to_hash(get_gpid())
                );
            _potential_secondary_states.delay_learning_task->enqueue(std::chrono::seconds(1));
        }
        else
        {
            handle_learning_error(err);
        }
        return;
    }

    if (resp.config.ballot > get_ballot())
    {
        ddebug("%s: on_learn_reply[%016llx]: first update configuration as ballot changed", name(), req.signature);
        bool ret = update_local_configuration(resp.config);
        dassert(ret, "");
    }

    if (status() != PS_POTENTIAL_SECONDARY)
    {
        derror("%s: on_learn_reply[%016llx]: current_state = %s, stop learning", name(), req.signature, enum_to_string(status()));
        return;
    }

    // local state is newer than learnee
    if (resp.last_committed_decree < _app->last_committed_decree())
    {
        dwarn(
            "%s: on_learn_reply[%016llx]: learnee = %s, learner state is newer than learnee (primary): %" PRId64 " vs %" PRId64 ", create new app",
            name(), req.signature, resp.config.primary.to_string(),
            _app->last_committed_decree(),
            resp.last_committed_decree            
            );

        // TODO(qinzuoyan):
        // - we'd better backup the old data, which may be recovered in some way.
        // - if we reuse the app object (not create a new object), the app must be reusable.
        auto err = _app->close(true);
        if (err != ERR_OK)
        {
            derror(
                "%s: on_learn_reply[%016llx]: learnee = %s, close app (with clear_state=true) failed, err = %s",
                name(), req.signature, resp.config.primary.to_string(),
                err.to_string()
                );
        }

        if (err == ERR_OK)
        {
            err = _app->open(true);
            if (err != ERR_OK)
            {
                derror(
                    "%s: on_learn_reply[%016llx]: learnee = %s, open app (with create_new=true) failed, err = %s",
                    name(), req.signature, resp.config.primary.to_string(),
                    err.to_string()
                    );
            }
        }

        if (err == ERR_OK)
        {            
            dassert(_app->last_committed_decree() == 0, "must be zero after app::open(true)");
            dassert(_app->last_durable_decree() == 0, "must be zero after app::open(true)");

            // reset prepare list
            _prepare_list->reset(0);

            err = _app->update_init_info(
                this,
                _stub->_log->on_partition_reset(get_gpid(), 0),
                _private_log ? _private_log->on_partition_reset(get_gpid(), 0) : 0
                );
            if (err != ERR_OK)
            {
                derror(
                    "%s: on_learn_reply[%016llx]: learnee = %s, update app init info failed, err = %s",
                    name(), req.signature, resp.config.primary.to_string(),
                    err.to_string()
                    );
            }
        }
        
        if (err != ERR_OK)
        {
            _potential_secondary_states.learn_remote_files_task = tasking::create_task(
                LPC_LEARN_REMOTE_DELTA_FILES,
                this,
                [this, err, req_cap = std::move(req), resp_cap = std::move(resp)]() mutable
                {
                    on_copy_remote_state_completed(err, 0, std::move(req_cap), std::move(resp_cap));
                }
                );
            _potential_secondary_states.learn_remote_files_task->enqueue();
            return;
        }
    }

    if (resp.prepare_start_decree != invalid_decree)
    {
        dassert(resp.type == LT_CACHE, "");
        dassert(resp.state.files.size() == 0, "");
        dassert(_potential_secondary_states.learning_status == LearningWithoutPrepare, "");
        _potential_secondary_states.learning_status = LearningWithPrepareTransient;

        // reset preparelist
        _potential_secondary_states.learning_start_prepare_decree = resp.prepare_start_decree;
        _prepare_list->reset(_app->last_committed_decree());
        ddebug(
            "%s: on_learn_reply[%016llx]: learnee = %s, reset_prepare_list = %" PRId64 ", current_learning_status = %s",
            name(), req.signature, resp.config.primary.to_string(),
            _app->last_committed_decree(),
            enum_to_string(_potential_secondary_states.learning_status)
            );
        
        // apply incoming prepare-list
        dassert(resp.state.meta.size() > 0, "learn mutation cache failed");
        binary_reader reader(resp.state.meta[0]);
        while (!reader.is_eof())
        {
            auto mu = mutation::read_from(reader, nullptr);
            mu->set_logged();
            dinfo("%s: on_learn_reply[%016llx]: apply learned mutation %s", name(), req.signature, mu->name());
            if (mu->data.header.decree > last_committed_decree())
                _prepare_list->prepare(mu, PS_POTENTIAL_SECONDARY);
        }

        // further states are synced using 2pc
        _prepare_list->commit(resp.prepare_start_decree - 1, COMMIT_TO_DECREE_HARD);        
        dassert(_prepare_list->last_committed_decree() == _app->last_committed_decree(), "");
        dassert(resp.state.files.size() == 0, "");

        // in-memory state is complete
        // still need on-disk state completion next with flush(true)
        dassert(_app->last_committed_decree() + 1 >= _potential_secondary_states.learning_start_prepare_decree,
            "state is incomplete");       

        // invalidate existing mutations in current logs
        err = _app->update_init_info(
            this,
            _stub->_log->on_partition_reset(get_gpid(), resp.prepare_start_decree - 1),
            _private_log ? _private_log->on_partition_reset(get_gpid(), resp.prepare_start_decree - 1) : 0
            );

        // go to next stage
        _potential_secondary_states.learning_status = LearningWithPrepare;        
        _potential_secondary_states.learn_remote_files_task = tasking::create_task(
            LPC_LEARN_REMOTE_DELTA_FILES,
            this,
            [this, err, req_cap = std::move(req), resp_cap = std::move(resp)]() mutable
            {
                on_copy_remote_state_completed(err, 0, std::move(req_cap), std::move(resp_cap));
            }
            );
        _potential_secondary_states.learn_remote_files_task->enqueue();
    }
   
    else if (resp.state.files.size() > 0)
    {
        utils::filesystem::remove_path(_app->learn_dir());
        utils::filesystem::create_directory(_app->learn_dir());

        _potential_secondary_states.learn_remote_files_task = 
            file::copy_remote_files(resp.config.primary,
                resp.base_local_dir,
                resp.state.files,
                _app->learn_dir(),
                true,
                LPC_REPLICATION_COPY_REMOTE_FILES,
                this,
                [this, req_cap = std::move(req), resp_copy = resp] (error_code err, int sz) mutable
                {
                    on_copy_remote_state_completed(err, sz, std::move(req_cap), std::move(resp_copy));
                }
                );
    }
    else
    {
        _potential_secondary_states.learn_remote_files_task = tasking::create_task(
            LPC_LEARN_REMOTE_DELTA_FILES,
            this,
            [this, req_cap = std::move(req), resp_cap = std::move(resp)]() mutable
            {
                on_copy_remote_state_completed(ERR_OK, 0, std::move(req_cap), std::move(resp_cap));
            }
            );
        _potential_secondary_states.learn_remote_files_task->enqueue();
    }
}

void replica::on_copy_remote_state_completed(
    error_code err,
    size_t size,
    learn_request&& req,
    learn_response&& resp
    )
{
    decree old_committed = _app->last_committed_decree();
    decree old_durable = _app->last_durable_decree();

    derror(
        "%s: on_copy_remote_state_completed[%016llx]: learner = %s, learn duration = %" PRIu64 " ms, err = %s, transfer %d files to %s",
        name(), req.signature, req.learner.to_string(),
        _potential_secondary_states.duration_ms(),
        err.to_string(),
        static_cast<int>(resp.state.files.size()),
        _dir.c_str()
        );

    if (err != ERR_OK)
    {
    }
    else if (_potential_secondary_states.learning_status == LearningWithPrepare)
    {
        dassert(resp.type == LT_CACHE, "");
    }
    else
    {
        dassert(resp.type == LT_APP || resp.type == LT_LOG, "");

        learn_state lstate;
        lstate.from_decree_excluded = resp.state.from_decree_excluded;
        lstate.to_decree_included = resp.state.to_decree_included;
        lstate.meta = resp.state.meta;
        
        for (auto& f : resp.state.files)
        {
            std::string file = utils::filesystem::path_combine(_app->learn_dir(), f);
            lstate.files.push_back(file);
        }

        // apply app learning
        if (resp.type == LT_APP)
        {
            auto start_ts = dsn_now_ns();
            err = _app->apply_checkpoint(lstate, CHKPT_LEARN);
            if (err == ERR_OK)
            {
                dassert(_app->last_committed_decree() >= _app->last_durable_decree(), "");
                // because if the original _app->last_committed_decree > resp.last_committed_decree,
                // the learn_start_decree will be set to 0, which makes learner to learn from scratch
                dassert(_app->last_committed_decree() <= resp.last_committed_decree, "");
                ddebug(
                    "%s: on_copy_remote_state_completed[%016llx]: learner = %s, learn duration = %" PRIu64 " ms, checkpoint duration = %" PRIu64 " ns, apply checkpoint succeed, app_last_committed_decree = %" PRId64,
                    name(), req.signature, req.learner.to_string(),
                    _potential_secondary_states.duration_ms(),
                    dsn_now_ns() - start_ts,
                    _app->last_committed_decree()
                    );
            }
            else
            {
                derror(
                    "%s: on_copy_remote_state_completed[%016llx]: learner = %s, learn duration = %" PRIu64 " ms, checkpoint duration = %" PRIu64 " ns, apply checkpoint failed, err = %s",
                    name(), req.signature, req.learner.to_string(),
                    _potential_secondary_states.duration_ms(),
                    dsn_now_ns() - start_ts,
                    err.to_string()
                    );
            }
        }

        // apply log learning
        else
        {
            auto start_ts = dsn_now_ns();
            err = apply_learned_state_from_private_log(lstate);
            if (err == ERR_OK)
            {
                ddebug(
                    "%s: on_copy_remote_state_completed[%016llx]: learner = %s, learn duration = %" PRIu64 " ms, apply learned state from private log succeed, duration = %" PRIu64 " ns",
                    name(), req.signature, req.learner.to_string(),
                    _potential_secondary_states.duration_ms(),
                    dsn_now_ns() - start_ts
                    );
            }
            else
            {
                derror(
                    "%s: on_copy_remote_state_completed[%016llx]: learner = %s, learn duration = %" PRIu64 " ms, apply learned state from private log failed, err = %s, duration = %" PRIu64 " ns",
                    name(), req.signature, req.learner.to_string(),
                    err.to_string(),
                    _potential_secondary_states.duration_ms(),
                    dsn_now_ns() - start_ts
                    );
            }
        }
    }

    ddebug(
        "%s: on_copy_remote_state_completed[%016llx], learning %d files to %s, err = %s, learn duration = %" PRIu64 " ms, "
        "appCommit(%" PRId64 " => %" PRId64 "), appDurable(%" PRId64 " => %" PRId64 "), localCommit(%" PRId64 "), "
        "remoteCommit(%" PRId64 "), prepareStart(%" PRId64 "), currentState(%s)",
        name(), 
        _potential_secondary_states.learning_signature,
        resp.state.files.size(), _dir.c_str(), err.to_string(),
        _potential_secondary_states.duration_ms(),
        old_committed, _app->last_committed_decree(),
        old_durable, _app->last_durable_decree(),
        last_committed_decree(), resp.last_committed_decree, resp.prepare_start_decree,
        enum_to_string(_potential_secondary_states.learning_status)
        );
    
    // if catch-up done, do flush to enable all learned state is durable
    if (err == ERR_OK
        && resp.prepare_start_decree != invalid_decree
        && _app->last_committed_decree() + 1 >= _potential_secondary_states.learning_start_prepare_decree
        && _app->last_committed_decree() > _app->last_durable_decree())
    {        
        err = _app->checkpoint();
        ddebug(
            "%s: on_copy_remote_state_completed[%016llx], flush done, err = %s, learn duration = %" PRIu64 " ms, lastC/DDecree = <%" PRId64 ", %" PRId64 ">",
            name(),
            _potential_secondary_states.learning_signature,
            err.to_string(), 
            _potential_secondary_states.duration_ms(),
            _app->last_committed_decree(), _app->last_durable_decree()
            );
        if (err == ERR_OK)
        {
            dassert(_app->last_committed_decree() == _app->last_durable_decree(), "");
        }
    }

    // it is possible that the _potential_secondary_states.learn_remote_files_task is still running
    // while its body is definitely done already as being here, so we manually set its value to nullptr
    // so that we don't have unnecessary failed reconfiguration later due to this non-nullptr in cleanup
    _potential_secondary_states.learn_remote_files_task = nullptr;
    
    _potential_secondary_states.learn_remote_files_completed_task = tasking::create_task(
        LPC_LEARN_REMOTE_DELTA_FILES_COMPLETED,
        this,
        [this, err]()
        {
            on_learn_remote_state_completed(err);
        },
        gpid_to_hash(get_gpid())
        );
    _potential_secondary_states.learn_remote_files_completed_task->enqueue();
}

void replica::on_learn_remote_state_completed(error_code err)
{
    check_hashed_access();

    if (PS_POTENTIAL_SECONDARY != status())
        return;

    ddebug(
        "%s: on_learn_remote_state_completed[%016llx], err = %s, learn duration = %" PRIu64 " ms, lastC/DDecree = <%" PRId64 ", %" PRId64 ">",
        name(),
        _potential_secondary_states.learning_signature,
        err.to_string(),
        _potential_secondary_states.duration_ms(),
        _app->last_committed_decree(), _app->last_durable_decree()
        );

    _potential_secondary_states.learning_round_is_running = false;

    if (err != ERR_OK)
    {
        handle_learning_error(err);
    }
    else
    {
        // continue
        init_learn(_potential_secondary_states.learning_signature);
    }
}

void replica::handle_learning_error(error_code err)
{
    check_hashed_access();

    derror(
        "%s: handle_learning_error[%016llx], err = %s, learn duration = %" PRIu64 " ms",
        name(),
        _potential_secondary_states.learning_signature,
        err.to_string(),
        _potential_secondary_states.duration_ms()
        );

    update_local_configuration_with_no_ballot_change(PS_ERROR);
}

void replica::handle_learning_succeeded_on_primary(
    ::dsn::rpc_address node, 
    uint64_t learn_signature
    )
{
    auto it = _primary_states.learners.find(node);
    if (it != _primary_states.learners.end()
        && it->second.signature == learn_signature
        )
    {
        upgrade_to_secondary_on_primary(node);
    }   
}

void replica::notify_learn_completion()
{
    group_check_response report;
    report.gpid = get_gpid();
    report.err = ERR_OK;
    report.last_committed_decree_in_app = _app->last_committed_decree();
    report.last_committed_decree_in_prepare_list = last_committed_decree();
    report.learner_signature = _potential_secondary_states.learning_signature;
    report.learner_status_ = _potential_secondary_states.learning_status;
    report.node = _stub->_primary_address;

    ddebug(
        "%s: notify_learn_completion with lastAppC/DDecree = <%" PRId64 ", %" PRId64 ">, "
        "lastCDecree = %" PRId64 ", learnState = %s",
        name(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        last_committed_decree(),
        enum_to_string(_potential_secondary_states.learning_status)
        );

    rpc::call_one_way_typed(_config.primary, RPC_LEARN_COMPLETION_NOTIFY, 
        report, gpid_to_hash(get_gpid()));
}

void replica::on_learn_completion_notification(const group_check_response& report)
{
    check_hashed_access();
    report.err.end_tracking();
    if (status() != PS_PRIMARY)
        return;

    if (report.learner_status_ == LearningSucceeded)
    {
        handle_learning_succeeded_on_primary(report.node, report.learner_signature);
    }
}

void replica::on_add_learner(const group_check_request& request)
{
    if (request.config.ballot < get_ballot())
    {
        ddebug("%s: %s ballot is old, skipped", name(), __FUNCTION__);
        return;
    }   

    if (request.config.ballot > get_ballot()
        || is_same_ballot_status_change_allowed(status(), request.config.status))
    {
        if (!update_local_configuration(request.config, true))
            return;

        dassert(PS_POTENTIAL_SECONDARY == status(), "");
        init_learn(request.config.learner_signature);
    }
}

error_code replica::apply_learned_state_from_private_log(learn_state& state)
{
    int64_t offset;
    error_code err;

    // temp prepare list for learning purpose
    prepare_list plist(
        _app->last_committed_decree(),
        _options->max_mutation_count_in_prepare_list,
        [this, &err](mutation_ptr& mu)
        {
            if (mu->data.header.decree == _app->last_committed_decree() + 1)
            {
                _app->write_internal(mu).end_tracking();
            }   
        }
        );

    err = mutation_log::replay(
        state.files,
        [this, &plist](mutation_ptr& mu)
        {
            auto d = mu->data.header.decree;
            if (d <= plist.last_committed_decree())
                return false;

            auto old = plist.get_mutation_by_decree(d);
            if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot)
                return false;

            plist.prepare(mu, PS_SECONDARY);
            return true;
        },
        offset
        );

    // apply in-buffer private logs
    if (err == ERR_OK && state.meta.size() > 0)
    {
        dassert(state.meta.size() == 1, "only 1 buffered private log is allowed");
        binary_reader reader(state.meta[0]);
        while (!reader.is_eof())
        {
            auto mu = mutation::read_from_log_file(reader, nullptr);
            auto d = mu->data.header.decree;
            if (d <= plist.last_committed_decree())
                continue;

            auto old = plist.get_mutation_by_decree(d);
            if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot)
                continue;
            
            mu->set_logged();
            plist.prepare(mu, PS_SECONDARY);            
        }
    }

    return err;
}

}} // namespace
