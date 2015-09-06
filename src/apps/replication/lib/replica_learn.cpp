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
#include "mutation_log.h"
#include "replica_stub.h"
#include <boost/filesystem.hpp>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.learn"

namespace dsn { namespace replication {

void replica::init_learn(uint64_t signature)
{
    check_hashed_access();

    dassert (status() == PS_POTENTIAL_SECONDARY, "");
        
    // at most one learning task running
    if (_potential_secondary_states.learning_round_is_running || !signature)
        return;

    if (signature != _potential_secondary_states.learning_signature)
    {
        // trigger flush but not need to wait
        int err = _app->flush(false);
        if (err != 0)
        {
            handle_learning_error(err);
            return;
        }
        _potential_secondary_states.cleanup(true);
        _potential_secondary_states.learning_signature = signature;
        _potential_secondary_states.learning_status = LearningWithoutPrepare;
        _prepare_list->reset(_app->last_committed_decree());
    }
    else
    {
        switch (_potential_secondary_states.learning_status)
        {
        case LearningSucceeded:
            notify_learn_completion();
            return;
        case LearningFailed:
            break;
        case LearningWithPrepare:
            if (_app->last_durable_decree() >= last_committed_decree())
            {
                _potential_secondary_states.learning_status = LearningSucceeded;
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

    // init learn request
    std::shared_ptr<learn_request> request(new learn_request);
    request->gpid = get_gpid();
    request->last_committed_decree_in_app = _app->last_committed_decree();
    request->last_committed_decree_in_prepare_list = _prepare_list->last_committed_decree();
    request->learner = primary_address();
    request->signature = _potential_secondary_states.learning_signature;
    _app->prepare_learning_request(request->app_specific_learn_request);

    _potential_secondary_states.learning_task = rpc::call_typed(
        _config.primary,
        RPC_LEARN,
        request,        
        this,
        &replica::on_learn_reply,
        gpid_to_hash(get_gpid())
        );

    ddebug(
        "%s: init_learn with lastAppC/DDecree = <%llu,%llu>, lastCDecree = %llu, learnState = %s",
        name(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        last_committed_decree(),
        enum_to_string(_potential_secondary_states.learning_status)
        );
}

// used by primary when received learn request from potential secondary
void replica::on_learn(const learn_request& request, __out_param learn_response& response)
{
    check_hashed_access();

    if (PS_PRIMARY != status())
    {
        response.err = ERR_INVALID_STATE;
        return;
    }
        
    _primary_states.get_replica_config(request.learner, response.config);

    auto it = _primary_states.learners.find(request.learner);
    if (it == _primary_states.learners.end())
    {
        response.err = (response.config.status == PS_SECONDARY ? ERR_OK : ERR_OBJECT_NOT_FOUND);
        return;
    }
    else if (it->second.signature != request.signature)
    {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    // prepare learnStartDecree
    decree localCommittedDecree = last_committed_decree();
    decree learnStartDecree = request.last_committed_decree_in_app + 1;
    if (request.last_committed_decree_in_app > localCommittedDecree)
    {
        ddebug(
            "%s: on_learn %s:%hu, learner state is lost due to DDD, "
            "with its appCommittedDecree = %llu vs localCommittedDecree = %llu, "
            "so we learn from scratch by setting learnStartDecree = 0",
            name(), request.learner.name, request.learner.port,
            request.last_committed_decree_in_app, localCommittedDecree
            );
        learnStartDecree = 0; // 0 means learn from scratch
    }

    ddebug(
        "%s: on_learn %s:%hu, with localCommittedDecree = %llu, "
        "localAppC/DDecree = <%llu, %llu>, learnStartDecree = %llu",
        name(), request.learner.name, request.learner.port,
        localCommittedDecree, _app->last_committed_decree(), _app->last_durable_decree(),
        learnStartDecree
        );
    
    response.prepare_start_decree = invalid_decree;
    response.commit_decree = localCommittedDecree;
    response.err = ERR_OK; 

    // set prepare_start_decree if need
    if (learnStartDecree + _options.staleness_for_start_prepare_for_potential_secondary > localCommittedDecree)
    {
        if (it->second.prepare_start_decree == invalid_decree)
        {
            // start from (last_committed_decree + 1)
            it->second.prepare_start_decree = localCommittedDecree + 1;

            // TODO(qinzuoyan) replay before response on_learn, learner may reject prepare request?
            cleanup_preparing_mutations(true);
            replay_prepare_list();

            ddebug(
                "%s: on_learn %s:%hu, set prepareStartDecree = %llu",
                name(), request.learner.name, request.learner.port,
                localCommittedDecree + 1
            );
        }

        response.prepare_start_decree = it->second.prepare_start_decree;
    }
    else
    {
        it->second.prepare_start_decree = invalid_decree;
    }

    int lerr = _app->get_learn_state(learnStartDecree, request.app_specific_learn_request, response.state);
    if (lerr != 0)
    {
        response.err = ERR_GET_LEARN_STATE_FALED;
        derror("%s get learn state failed, error = %d", dir().c_str(), lerr);
    }
    else
    {
        response.base_local_dir = _app->data_dir();
        for (auto itr = response.state.files.begin(); itr != response.state.files.end(); ++itr)
            *itr = itr->substr(_app->data_dir().length());
    }
}

// used by potential secondary when received learn reply from primary
void replica::on_learn_reply(error_code err, std::shared_ptr<learn_request>& req, std::shared_ptr<learn_response>& resp)
{
    check_hashed_access();

    dassert(PS_POTENTIAL_SECONDARY == status(), "");
    dassert(req->signature == _potential_secondary_states.learning_signature, "");

    if (err != ERR_OK)
    {
        handle_learning_error(err);
        return;
    }

    ddebug(
        "%s: on_learn_reply with err = %s, remoteCommit = %llu, prepareStart = %llu, currentState = %s",
        name(), resp->err.to_string(), resp->commit_decree, resp->prepare_start_decree,
        enum_to_string(_potential_secondary_states.learning_status)
        );

    if (resp->err != ERR_OK)
    {
        handle_learning_error(resp->err);
        return;
    }

    if (resp->config.ballot > get_ballot())
    {
        update_local_configuration(resp->config);
    }

    if (status() != PS_POTENTIAL_SECONDARY)
    {
        return;
    }

    if (resp->prepare_start_decree != invalid_decree && _potential_secondary_states.learning_status == LearningWithoutPrepare)
    {
        _potential_secondary_states.learning_status = LearningWithPrepare;
        _prepare_list->reset(resp->prepare_start_decree - 1);
        ddebug(
            "%s: resetPrepareList = %llu, currentState = %s",
            name(), resp->prepare_start_decree - 1,
            enum_to_string(_potential_secondary_states.learning_status)
            );
    }

    if (resp->state.files.size() > 0)
    {
        _potential_secondary_states.learn_remote_files_task = 
            file::copy_remote_files(resp->config.primary,
                resp->base_local_dir,
                resp->state.files,
                _app->learn_dir(),
                true,
                LPC_COPY_REMOTE_DELTA_FILES,
                this,
                std::bind(&replica::on_copy_remote_state_completed, this,
                std::placeholders::_1,
                std::placeholders::_2,
                resp)
                );
    }
    else
    {
        _potential_secondary_states.learn_remote_files_task = tasking::enqueue(
            LPC_LEARN_REMOTE_DELTA_FILES,
            this,
            std::bind(&replica::on_copy_remote_state_completed, this, ERR_OK, 0, resp)
            );
    }
}

void replica::on_copy_remote_state_completed(error_code err2, size_t size, std::shared_ptr<learn_response> resp)
{   
    learn_state localState;
    localState.meta = resp->state.meta;
    if (err2 == ERR_OK)
    {
        // flush before learn
        int err = _app->flush(true);

        if (err == 0)
        {
            std::string file;
            if (dir().back() == '/' || itr->front() == '/')
                file = dir() + *itr;
            else
                file = dir() + '/' + *itr;

            localState.files.push_back(file);
        }

        decree oldDecree = _app->last_committed_decree();
        decree oldDurable = _app->last_durable_decree();

        // the only place where there is non-in-partition-thread update
        int err = _app->apply_learn_state(resp->state);
        if (err == 0)
        {
            dassert(_app->last_committed_decree() >= _app->last_durable_decree(), "");
            // because if the original _app->last_committed_decree > resp->commit_decree,
            // the learnStartDecree will be set to 0, which makes learner to learn from scratch
            dassert(_app->last_committed_decree() <= resp->commit_decree, "");
        }

        ddebug(
                "%s: learning %d files to %s, err = %x, "
                "appCommit(%llu => %llu), appDurable(%llu => %llu), "
                "remoteCommit(%llu), prepareStart(%llu), currentState(%s)",
                name(), resp->state.files.size(), _dir.c_str(), err,
                oldDecree, _app->last_committed_decree(),
                oldDurable, _app->last_durable_decree(),
                resp->commit_decree, resp->prepare_start_decree,
                enum_to_string(_potential_secondary_states.learning_status)
              );

        // if catch-up done, do flush to force data in memtables to the ground
        if (err == 0 && _app->last_committed_decree() == resp->commit_decree)
        {
            err = _app->flush(true);
            ddebug(
                "%s: flush done, err = %d, lastC/DDecree = <%llu, %llu>",
                name(), err, _app->last_committed_decree(), _app->last_durable_decree()
                );
            if (err == 0)
            {
                // flush after learn
                err = _app->flush(true);
            }
        }

        // translate to general error code
        if (err == 0)
        {
            dassert (_app->last_committed_decree() == _app->last_durable_decree(), "");
        }
        else
        {
            err2 = ERR_LOCAL_APP_FAILURE;
        }
    } 
    else 
    {
        derror(
                "%s: transfer %d files to %s failed, err = %s",
                name(),
                static_cast<int>(resp->state.files.size()), 
                _dir.c_str(), 
                err2.to_string()
                );
    }    

    _potential_secondary_states.learn_remote_files_completed_task = tasking::enqueue(
        LPC_LEARN_REMOTE_DELTA_FILES_COMPLETED,
        this,
        std::bind(&replica::on_learn_remote_state_completed, this, err2),
        gpid_to_hash(get_gpid())
        );
}

void replica::on_learn_remote_state_completed(error_code err)
{
    check_hashed_access();
    
    if (PS_POTENTIAL_SECONDARY != status())
        return;

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

    dwarn(
        "%s: learning failed with err = %s, LastCommitted = %lld",
        name(),
        err.to_string(),
        _app->last_committed_decree()
        );

    _potential_secondary_states.cleanup(true);
    _potential_secondary_states.learning_status = LearningFailed;

    update_local_configuration_with_no_ballot_change(PS_ERROR);
}

void replica::handle_learning_succeeded_on_primary(const dsn_address_t& node, uint64_t learnSignature)
{
    auto it = _primary_states.learners.find(node);
    if (it != _primary_states.learners.end() && it->second.signature == learnSignature)
        upgrade_to_secondary_on_primary(node);
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
    report.node = primary_address();

    ddebug(
        "%s: notify_learn_completion with lastAppC/DDecree = <%llu,%llu>, lastCDecree = %llu, learnState = %s",
        name(),
        _app->last_committed_decree(),
        _app->last_durable_decree(),
        last_committed_decree(),
        enum_to_string(_potential_secondary_states.learning_status)
        );

    rpc::call_one_way_typed(_config.primary, RPC_LEARN_COMPLETION_NOTIFY, report, gpid_to_hash(get_gpid()));
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
        return;

    if (request.config.ballot > get_ballot()
        || is_same_ballot_status_change_allowed(status(), request.config.status))
    {
        update_local_configuration(request.config, true);
        dassert(PS_POTENTIAL_SECONDARY == status(), "");
        init_learn(request.learner_signature);
    }
}

}} // namespace
