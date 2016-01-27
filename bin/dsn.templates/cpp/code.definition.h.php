<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>
# pragma once
# include <dsn/service_api_cpp.h>
# include "<?=$_PROG->name?>.types.h"

<?php
echo $_PROG->get_cpp_namespace_begin().PHP_EOL;

foreach ($_PROG->services as $svc)
{
    echo "    // define your own thread pool using DEFINE_THREAD_POOL_CODE(xxx)". PHP_EOL;    
    echo "    // define RPC task code for service '". $svc->name ."'". PHP_EOL;
    foreach ($svc->functions as $f)
    {
        echo "    DEFINE_TASK_CODE_RPC(". $f->get_rpc_code() 
            . ", TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)".PHP_EOL;
    }
}
echo "    // test timer task code".PHP_EOL; 
echo "    DEFINE_TASK_CODE(". $_PROG->get_test_task_code() 
        . ", TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)".PHP_EOL;

echo $_PROG->get_cpp_namespace_end().PHP_EOL;
?>
